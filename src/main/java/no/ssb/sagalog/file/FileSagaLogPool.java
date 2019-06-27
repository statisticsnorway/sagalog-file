package no.ssb.sagalog.file;


import no.ssb.sagalog.AbstractSagaLogPool;
import no.ssb.sagalog.SagaLog;
import no.ssb.sagalog.SagaLogBusyException;
import no.ssb.sagalog.SagaLogId;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Set;
import java.util.stream.Collectors;

class FileSagaLogPool extends AbstractSagaLogPool {

    private final Path folder;

    FileSagaLogPool(Path folder, String clusterInstanceId) {
        super(clusterInstanceId);
        this.folder = folder;
    }

    @Override
    public FileSagaLogId idFor(String clusterInstanceId, String logName) {
        return new FileSagaLogId(folder, clusterInstanceId, logName);
    }

    @Override
    public Set<SagaLogId> clusterWideLogIds() {
        try {
            return Files.list(folder)
                    .filter(p -> p.toFile().getName().endsWith(".sagalog"))
                    .map(Path::toAbsolutePath)
                    .map(path -> new FileSagaLogId(path))
                    .collect(Collectors.toSet());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    protected SagaLog connectExternal(SagaLogId logId) throws SagaLogBusyException {
        return new FileSagaLog(logId);
    }

    @Override
    protected boolean deleteExternal(SagaLogId logId) {
        try {
            return Files.deleteIfExists(((FileSagaLogId) logId).getPath());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
