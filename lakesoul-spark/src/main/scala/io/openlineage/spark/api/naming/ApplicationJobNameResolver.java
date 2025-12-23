//
// Source code recreated from a .class file by IntelliJ IDEA
// (powered by FernFlower decompiler)
//

package io.openlineage.spark.api.naming;

import com.google.common.collect.ImmutableList;
import io.openlineage.spark.api.OpenLineageContext;
import java.util.List;
import java.util.Locale;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class ApplicationJobNameResolver {
    private static final Logger log = LoggerFactory.getLogger(ApplicationJobNameResolver.class);
    private final List<ApplicationJobNameProvider> applicationJobNameProviders;

    public String getJobName(OpenLineageContext olContext) {
        return (String)this.applicationJobNameProviders.stream().filter((provider) -> provider.isDefinedAt(olContext)).findFirst().map((provider) -> provider.getJobName(olContext)).map(ApplicationJobNameResolver::normalizeName).orElseThrow(() -> new IllegalStateException("None of the job providers was able to provide the job name. The number of job providers is " + this.applicationJobNameProviders.size()));
    }

    public static List<ApplicationJobNameProvider> buildProvidersList() {
        return ImmutableList.of(new OpenLineageAppNameApplicationJobNameProvider(), new AwsGlueApplicationJobNameProvider(), new SparkApplicationNameApplicationJobNameProvider());
    }

    private static String normalizeName(String name) {
        String normalizedName = name.replaceAll("[\\s\\-_]?((?<=.)[A-Z](?=[a-z\\s\\-_])|(?<=[^A-Z])[A-Z]|((?<=[\\s\\-_])[a-z\\d]))", "_$1").toLowerCase(Locale.ROOT);
        log.debug("The application name [{}] has been normalized to [{}]", name, normalizedName);
        if (normalizedName.contains("batch")){
            normalizedName = normalizedName.replaceAll("_\\d+_batch$", "");
        }
        return normalizedName;
    }

    public ApplicationJobNameResolver(List<ApplicationJobNameProvider> applicationJobNameProviders) {
        this.applicationJobNameProviders = applicationJobNameProviders;
    }
}
