package in.indigo.util;

import java.util.List;

import in.indigo.entity.GstDistributionConfiguration;
import in.indigo.entity.InvGLExtractdwh;
import in.indigo.entity.InvSkyExtract;
import in.indigo.entity.PkgAudit;
import in.indigo.repository.GstDistributionConfigurationRepository;
import in.indigo.repository.InvGlExtractdwhRepository;
import in.indigo.repository.InvSkyExtractRepository;
import in.indigo.repository.PkgAuditRepository;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.transaction.Transactional;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@ApplicationScoped
public class RepositoryUtil {

    @Inject
    GstDistributionConfigurationRepository gstDistributionConfigurationRepository;

    @Inject
    InvSkyExtractRepository invSkyExtractRepository;

    @Inject
    PkgAuditRepository pkgAuditRepository;

    @Inject
    InvGlExtractdwhRepository invGlExtractdwhRepository;

    @Transactional
    public GstDistributionConfiguration getGstRate(String str) {
        List<GstDistributionConfiguration> gstConf = gstDistributionConfigurationRepository
                .findByPlaceOfEmbarkation(str);

        if (gstConf.size() > 0)
            return gstConf.get(0);
        else
            return null;
    }

    @Transactional
    public List<GstDistributionConfiguration> getGstRate() {
        List<GstDistributionConfiguration> gstConf = gstDistributionConfigurationRepository
                .findByDistinctPlaceOfEmbarkation();
        return gstConf;
    }

    @Transactional
    public void dumpBatch(List<InvSkyExtract> batch) {
        try {
            invSkyExtractRepository.persist(batch);
        } catch (Exception e) {
            System.err.println("General error occurred: " + e.getMessage());
            throw new RuntimeException("General error", e);
        }
    }

    @Transactional
    public void dumpPkgAudit(PkgAudit pkgAudit) {
        pkgAuditRepository.persist(pkgAudit);
        log.info("data dumped into PkgAudit");

    }

    @Transactional
    public void dumpGl(InvGLExtractdwh invGLExtractdwh) {
        invGlExtractdwhRepository.persist(invGLExtractdwh);
        log.info("data dumped into Gl");
    }

}
