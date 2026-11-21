package com.sentinel.reservation.reservation;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.support.TransactionTemplate;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

/**
 * Releases holds whose payment never completed (client vanished, payment provider down, consumer dead).
 * This is the safety net that guarantees inventory is never stuck: every PENDING_PAYMENT reservation
 * ends in a terminal state within holdTtl + sweep interval.
 *
 * <p>Each batch runs in one transaction: SKIP LOCKED claims rows that no other sweeper or payment
 * confirmation is touching right now; rows currently being confirmed are simply skipped and revisited.
 */
@Component
public class HoldExpirySweeper {

    private static final Logger log = LoggerFactory.getLogger(HoldExpirySweeper.class);
    private static final int BATCH = 100;

    private final ReservationRepository reservations;
    private final ReservationService service;
    private final TransactionTemplate tx;

    public HoldExpirySweeper(ReservationRepository reservations, ReservationService service, TransactionTemplate tx) {
        this.reservations = reservations;
        this.service = service;
        this.tx = tx;
    }

    @Scheduled(fixedDelayString = "${sentinel.sweeper.interval:5s}")
    public void sweep() {
        int n;
        do {
            n = sweepOnce(Instant.now());
        } while (n == BATCH);
    }

    public int sweepOnce(Instant now) {
        Integer expired = tx.execute(s -> {
            List<UUID> ids = reservations.lockExpiredHolds(now, BATCH);
            ids.forEach(service::expireLocked); // joins this transaction; row already locked
            return ids.size();
        });
        if (expired != null && expired > 0) {
            log.info("Expired {} unpaid holds", expired);
        }
        return expired == null ? 0 : expired;
    }
}
