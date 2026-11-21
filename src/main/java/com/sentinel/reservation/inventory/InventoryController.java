package com.sentinel.reservation.inventory;

import jakarta.validation.Valid;
import jakarta.validation.constraints.DecimalMin;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Pattern;
import jakarta.validation.constraints.Size;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.format.annotation.DateTimeFormat;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.UUID;

@RestController
public class InventoryController {

    private final InventoryRepository repo;

    public InventoryController(InventoryRepository repo) {
        this.repo = repo;
    }

    public record CreateHotel(@NotBlank @Size(max = 200) String name,
                              @NotNull @Size(min = 1, max = 20) List<@Valid CreateRoomType> roomTypes) {}

    public record CreateRoomType(@NotBlank @Size(max = 100) String name, @NotNull @Min(1) @Max(20) Integer maxGuests) {}

    public record HotelCreated(UUID hotelId, List<RoomTypeCreated> roomTypes) {}

    public record RoomTypeCreated(UUID roomTypeId, String name) {}

    @PostMapping("/api/v1/admin/hotels")
    @Transactional
    public ResponseEntity<HotelCreated> createHotel(@Valid @RequestBody CreateHotel req) {
        UUID hotelId = UUID.randomUUID();
        repo.insertHotel(hotelId, req.name());
        List<RoomTypeCreated> rts = req.roomTypes().stream().map(rt -> {
            UUID id = UUID.randomUUID();
            repo.insertRoomType(id, hotelId, rt.name(), rt.maxGuests());
            return new RoomTypeCreated(id, rt.name());
        }).toList();
        return ResponseEntity.status(HttpStatus.CREATED).body(new HotelCreated(hotelId, rts));
    }

    public record SetInventory(@NotNull LocalDate from, @NotNull LocalDate to, @NotNull @Min(0) @Max(10000) Integer total,
                               @NotNull @DecimalMin("0.01") BigDecimal price, @NotBlank @Pattern(regexp = "[A-Z]{3}") String currency) {}

    @PutMapping("/api/v1/admin/room-types/{roomTypeId}/inventory")
    @Transactional
    public ResponseEntity<?> setInventory(@PathVariable UUID roomTypeId, @Valid @RequestBody SetInventory req) {
        if (!req.to().isAfter(req.from()) || ChronoUnit.DAYS.between(req.from(), req.to()) > 730) {
            throw new IllegalArgumentException("'to' must be after 'from' and the range at most 730 nights");
        }
        repo.findRoomType(roomTypeId).orElseThrow(() -> new ResponseStatusException(HttpStatus.NOT_FOUND, "room type not found"));
        try {
            int nights = repo.upsertNights(roomTypeId, req.from(), req.to(), req.total(), req.price(), req.currency());
            return ResponseEntity.ok(java.util.Map.of("nightsUpdated", nights));
        } catch (DataIntegrityViolationException e) {
            throw new ResponseStatusException(HttpStatus.CONFLICT, "total cannot be lower than rooms already reserved");
        }
    }

    public record Availability(UUID roomTypeId, LocalDate checkIn, LocalDate checkOut, int availableRooms,
                               BigDecimal pricePerRoom, String currency, List<InventoryRepository.Night> nights) {}

    /** Public, lock-free availability read. Booking re-validates under row locks. */
    @GetMapping("/api/v1/availability")
    public Availability availability(@RequestParam UUID roomTypeId,
                                     @RequestParam @DateTimeFormat(iso = DateTimeFormat.ISO.DATE) LocalDate checkIn,
                                     @RequestParam @DateTimeFormat(iso = DateTimeFormat.ISO.DATE) LocalDate checkOut) {
        if (!checkOut.isAfter(checkIn) || ChronoUnit.DAYS.between(checkIn, checkOut) > 30) {
            throw new IllegalArgumentException("checkOut must be after checkIn and the stay at most 30 nights");
        }
        var nights = repo.nights(roomTypeId, checkIn, checkOut);
        boolean complete = nights.size() == ChronoUnit.DAYS.between(checkIn, checkOut);
        int available = complete ? nights.stream().mapToInt(InventoryRepository.Night::available).min().orElse(0) : 0;
        BigDecimal price = nights.stream().map(InventoryRepository.Night::price).reduce(BigDecimal.ZERO, BigDecimal::add);
        String currency = nights.isEmpty() ? null : nights.get(0).currency();
        return new Availability(roomTypeId, checkIn, checkOut, available, complete ? price : null, currency, nights);
    }

    @GetMapping("/api/v1/admin/inventory/audit")
    public java.util.Map<String, Object> audit() {
        var drift = repo.audit();
        return java.util.Map.of("consistent", drift.isEmpty(), "violations", drift);
    }
}
