package no.ssb.sagalog.file;


import no.ssb.sagalog.SagaLog;
import no.ssb.sagalog.SagaLogAlreadyAquiredByOtherOwnerException;
import no.ssb.sagalog.SagaLogId;
import no.ssb.sagalog.SagaLogOwner;
import no.ssb.sagalog.SagaLogOwnership;
import no.ssb.sagalog.SagaLogPool;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.ZonedDateTime;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

class FileSagaLogPool implements SagaLogPool {

    private final Map<SagaLogId, FileSagaLog> sagaLogByLogId = new ConcurrentHashMap<>();
    private final Map<SagaLogId, SagaLogOwnership> ownershipByLogId = new ConcurrentHashMap<>();

    private final Path folder;
    private final String clusterInstanceId;

    FileSagaLogPool(Path folder, String clusterInstanceId) {
        this.folder = folder;
        this.clusterInstanceId = clusterInstanceId;
    }

    @Override
    public SagaLogId idFor(String internalId) {
        Path absolutePath = Paths.get(folder.toString(), clusterInstanceId + "-" + internalId + ".sagalog").toAbsolutePath();
        return new SagaLogId(absolutePath.toString());
    }

    @Override
    public Set<SagaLogId> clusterWideLogIds() {
        try {
            return Files.list(folder)
                    .filter(p -> p.toFile().getName().endsWith(".sagalog"))
                    .map(Path::toAbsolutePath)
                    .map(Path::toString)
                    .map(SagaLogId::new)
                    .collect(Collectors.toSet());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Set<SagaLogId> instanceLocalLogIds() {
        return new LinkedHashSet<>(sagaLogByLogId.keySet());
    }

    @Override
    public Set<SagaLogOwnership> instanceLocalSagaLogOwnerships() {
        return new LinkedHashSet<>(ownershipByLogId.values());
    }

    @Override
    public SagaLog connect(SagaLogId logId) {
        FileSagaLog sagaLog = sagaLogByLogId.computeIfAbsent(logId, lid -> new FileSagaLog(logId));
        return sagaLog;
    }

    @Override
    public void remove(SagaLogId logId) {
        release(logId);
        FileSagaLog sagaLog = sagaLogByLogId.remove(logId);
        if (sagaLog != null) {
            try {
                sagaLog.close();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    @Override
    public SagaLog acquire(SagaLogOwner owner, SagaLogId logId) throws SagaLogAlreadyAquiredByOtherOwnerException {
        SagaLogOwnership ownership = ownershipByLogId.computeIfAbsent(logId, id -> new SagaLogOwnership(owner, id, ZonedDateTime.now()));
        if (!owner.equals(ownership.getOwner())) {
            throw new SagaLogAlreadyAquiredByOtherOwnerException(String.format("SagaLogOwner %s was unable to acquire saga-log with id %s. Already owned by %s.", owner.getOwnerId(), logId, ownership.getOwner().getOwnerId()));
        }
        return connect(logId);
    }

    @Override
    public void release(SagaLogOwner owner) {
        ownershipByLogId.values().removeIf(ownership -> owner.equals(ownership.getOwner()));
    }

    @Override
    public void release(SagaLogId logId) {
        ownershipByLogId.remove(logId);
    }

    @Override
    public void shutdown() {
        for (FileSagaLog sagaLog : sagaLogByLogId.values()) {
            try {
                sagaLog.close();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
        sagaLogByLogId.clear();
        ownershipByLogId.clear();
    }
}
