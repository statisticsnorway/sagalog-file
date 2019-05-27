import no.ssb.sagalog.SagaLogInitializer;
import no.ssb.sagalog.file.FileSagaLogInitializer;

module no.ssb.sagalog.file {
    requires no.ssb.sagalog;
    requires tape;

    provides SagaLogInitializer with FileSagaLogInitializer;
}
