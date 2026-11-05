package com.mservice.farmstream.device;

import com.mservice.farmstream.util.Config;
import io.hydrangea.Backoff;
import io.hydrangea.EventLoopGroup;
import io.hydrangea.log.Logger;
import io.hydrangea.postgres.PgConfig;
import io.hydrangea.postgres.PgPool;
import io.hydrangea.postgres.PgResult;
import io.hydrangea.postgres.PgRow;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * The {@code aegis_shield.devices} table in PostgreSQL (hydrangea-postgres). When the database cannot be reached at startup
 * the service keeps working in memory and every write becomes a no-op, like the Node version which only logged the errors.
 * Env: DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD, DB_DISABLED=true.
 */
public final class DeviceRepository {

    private static final Logger LOG = Logger.get(DeviceRepository.class);

    public record DeviceRecord(String serial, String label, String sdkVersion, String connectType, String ipv4,
                               String status, String usedBy) {
    }

    private final PgPool pool; // null = no database

    private DeviceRepository(PgPool pool) {
        this.pool = pool;
    }

    public static DeviceRepository connect(EventLoopGroup group) {
        if (Config.getBool("DB_DISABLED", false)) {
            LOG.info("database disabled (DB_DISABLED=true)");
            return new DeviceRepository(null);
        }
        String password = Config.get("DB_PASSWORD");
        if (password == null) {
            LOG.error("DB_PASSWORD not set, continuing without persistence (set DB_PASSWORD or DB_DISABLED=true)");
            return new DeviceRepository(null);
        }
        PgConfig config = PgConfig.of(
                        Config.get("DB_HOST", "127.0.0.1"),
                        Config.getInt("DB_PORT", 5432),
                        Config.get("DB_USER", "fs_core"),
                        Config.get("DB_NAME", "fs_core"))
                .withPassword(password)
                .withConnectTimeout(Duration.ofSeconds(5))
                .withQueryTimeout(Duration.ofSeconds(15))
                .withApplicationName("farm-stream")
                .withReconnect(Backoff.defaults());
        try {
            PgPool pool = PgPool.create(group, config, 2).get(10, TimeUnit.SECONDS);
            LOG.info("connected to PostgreSQL {}:{}/{}", config.host(), config.port(), config.database());
            return new DeviceRepository(pool);
        } catch (Exception e) {
            Throwable cause = e.getCause() != null ? e.getCause() : e;
            LOG.error("database unavailable, continuing without persistence: {}", cause.toString());
            return new DeviceRepository(null);
        }
    }

    public boolean available() {
        return pool != null;
    }

    public void close() {
        if (pool != null) {
            pool.close();
        }
    }

    public CompletableFuture<Void> ensureTable() {
        if (pool == null) {
            return CompletableFuture.completedFuture(null);
        }
        return pool.execute("""
                CREATE SCHEMA IF NOT EXISTS aegis_shield;
                CREATE TABLE IF NOT EXISTS aegis_shield.devices (
                    id             BIGSERIAL    PRIMARY KEY,
                    serial         VARCHAR(255) NOT NULL,
                    label          VARCHAR(255),
                    sdk_version    VARCHAR(50),
                    connect_type   VARCHAR(10),
                    ipv4           VARCHAR(50),
                    status         VARCHAR(20)  NOT NULL DEFAULT 'DISCONNECT',
                    used_by        VARCHAR(255),
                    last_seen_at   TIMESTAMPTZ,
                    created_at     TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
                    updated_at     TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
                    CONSTRAINT uq_devices_serial UNIQUE (serial)
                );
                ALTER TABLE aegis_shield.devices ADD COLUMN IF NOT EXISTS used_by VARCHAR(255);
                CREATE INDEX IF NOT EXISTS idx_devices_serial ON aegis_shield.devices (serial);
                CREATE INDEX IF NOT EXISTS idx_devices_status ON aegis_shield.devices (status);
                """).thenApply(r -> null);
    }

    /** Insert or update; null fields keep their stored value (except used_by, which is only written when non-null). */
    public CompletableFuture<Void> upsert(String serial, String label, String sdkVersion, String connectType, String ipv4,
                                          String status, String usedBy, boolean touchLastSeen) {
        if (pool == null) {
            return CompletableFuture.completedFuture(null);
        }
        return pool.query("""
                        INSERT INTO aegis_shield.devices
                            (serial, label, sdk_version, connect_type, ipv4, status, used_by, last_seen_at, updated_at)
                        VALUES ($1, $2, $3, $4, $5, COALESCE($6, 'DISCONNECT'), $7, CASE WHEN $8 THEN NOW() ELSE NULL END, NOW())
                        ON CONFLICT (serial) DO UPDATE SET
                            connect_type  = COALESCE($4, aegis_shield.devices.connect_type),
                            ipv4          = COALESCE($5, aegis_shield.devices.ipv4),
                            status        = COALESCE($6, aegis_shield.devices.status),
                            used_by       = CASE WHEN $7 IS NOT NULL THEN $7 ELSE aegis_shield.devices.used_by END,
                            last_seen_at  = CASE WHEN $8 THEN NOW() ELSE aegis_shield.devices.last_seen_at END,
                            sdk_version   = COALESCE($3, aegis_shield.devices.sdk_version),
                            label         = COALESCE($2, aegis_shield.devices.label),
                            updated_at    = NOW()
                        """, serial, label, sdkVersion, connectType, ipv4, status, usedBy, touchLastSeen)
                .thenApply(r -> (Void) null)
                .whenComplete((v, e) -> {
                    if (e != null) {
                        LOG.warn("upsert failed for {}: {}", serial, e.getMessage());
                    }
                });
    }

    /** Status update; {@code updateUser = false} leaves used_by alone. */
    public CompletableFuture<Void> setStatus(String serial, String status, boolean updateUser, String usedBy) {
        if (pool == null) {
            return CompletableFuture.completedFuture(null);
        }
        CompletableFuture<PgResult> f = updateUser
                ? pool.query("UPDATE aegis_shield.devices SET status = $1, used_by = $2, updated_at = NOW() WHERE serial = $3", status, usedBy, serial)
                : pool.query("UPDATE aegis_shield.devices SET status = $1, updated_at = NOW() WHERE serial = $2", status, serial);
        return f.thenApply(r -> (Void) null).whenComplete((v, e) -> {
            if (e != null) {
                LOG.warn("setStatus failed for {}: {}", serial, e.getMessage());
            }
        });
    }

    public CompletableFuture<List<DeviceRecord>> findAll() {
        if (pool == null) {
            return CompletableFuture.completedFuture(List.of());
        }
        return pool.query("SELECT serial, label, sdk_version, connect_type, ipv4, status, used_by FROM aegis_shield.devices ORDER BY updated_at DESC")
                .thenApply(result -> {
                    List<DeviceRecord> list = new ArrayList<>();
                    for (PgRow r : result.rows()) {
                        list.add(new DeviceRecord(r.getString("serial"), r.getString("label"), r.getString("sdk_version"),
                                r.getString("connect_type"), r.getString("ipv4"), r.getString("status"), r.getString("used_by")));
                    }
                    return list;
                });
    }
}
