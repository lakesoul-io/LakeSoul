// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0
package org.apache.flink.lakesoul.entry.sql.flink;


import io.openlineage.client.OpenLineage;
import io.openlineage.client.OpenLineageClient;
import io.openlineage.client.transports.HttpConfig;
import io.openlineage.client.transports.HttpTransport;
import io.openlineage.client.transports.Transport;
import io.openlineage.flink.client.EventEmitter;
import io.openlineage.flink.shaded.org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.core.execution.DetachedJobExecutionResult;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.core.execution.JobListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.net.URI;
import java.time.ZonedDateTime;
import java.util.*;

public class LakeSoulInAndOutputJobListener implements JobListener {
    private static final Logger log = LoggerFactory.getLogger(LakeSoulInAndOutputJobListener.class);
    private OpenLineageClient client;
    private OpenLineage openLineage;
    private List<OpenLineage.InputDataset> inputDatasets;
    private List<OpenLineage.OutputDataset> outputDatasets;
    private String executeMode = "STREAM";
    private UUID runId;
    OpenLineage.Run run;
    private OpenLineage.Job job;

    public LakeSoulInAndOutputJobListener(String url) {
        HttpConfig config = new HttpConfig();
        config.setUrl(URI.create(url));
        Transport transport = new HttpTransport(config);
        client = new OpenLineageClient(transport);
        openLineage = new OpenLineage(EventEmitter.OPEN_LINEAGE_CLIENT_URI);
    }

    public LakeSoulInAndOutputJobListener(String url, String executeMode) {
        this(url);
        this.executeMode = executeMode;
    }

    public LakeSoulInAndOutputJobListener jobName(String name, String namespace) {
        this.runId = UUID.randomUUID();
        this.run = openLineage.newRunBuilder().runId(this.runId).build();
        OpenLineage.JobFacets jobFacets = openLineage.newJobFacetsBuilder().jobType(openLineage.newJobTypeJobFacetBuilder().jobType("Flink Job").integration("Flink").processingType(this.executeMode).build()).build();
        this.job = openLineage.newJobBuilder().name(name).namespace(namespace).facets(jobFacets).build();
        return this;
    }

    public LakeSoulInAndOutputJobListener inputFacets(String inputName, String inputNamespace, String[] inputSchemaNames, String[] inputSchemaTypes) {
        List<OpenLineage.SchemaDatasetFacetFields> schemaFields = new ArrayList<>();
        if (inputSchemaNames != null && inputSchemaTypes != null && inputSchemaTypes.length == inputSchemaTypes.length) {
            for (int i = 0; i < inputSchemaNames.length; i++) {
                schemaFields.add(openLineage.newSchemaDatasetFacetFieldsBuilder().name(inputSchemaNames[i]).type(inputSchemaTypes[i]).build());
            }
        }
        if (inputSchemaNames != null && inputSchemaTypes == null) {
            for (int i = 0; i < inputSchemaNames.length; i++) {
                schemaFields.add(openLineage.newSchemaDatasetFacetFieldsBuilder().name(inputSchemaNames[i]).build());
            }
        }

        OpenLineage.SchemaDatasetFacet schemaFacet = openLineage.newSchemaDatasetFacetBuilder().fields(schemaFields).build();
        this.inputDatasets = Arrays.asList(
                openLineage.newInputDatasetBuilder().name(inputName).namespace(inputNamespace)
                        .facets(
                                openLineage.newDatasetFacetsBuilder().schema(schemaFacet).build()
                        ).build()
        );
        return this;
    }

    public LakeSoulInAndOutputJobListener outputFacets(String outputName, String outputNamespace, String[] outputSchemaNames, String[] outputSchemaTypes) {

        List<OpenLineage.SchemaDatasetFacetFields> schemaFields = new ArrayList<>();
        if (outputSchemaNames != null && outputSchemaTypes != null && outputSchemaTypes.length == outputSchemaTypes.length) {
            for (int i = 0; i < outputSchemaNames.length; i++) {
                schemaFields.add(openLineage.newSchemaDatasetFacetFieldsBuilder().name(outputSchemaNames[i]).type(outputSchemaTypes[i]).build());
            }
        }
        if (outputSchemaNames != null && outputSchemaTypes == null) {
            for (int i = 0; i < outputSchemaNames.length; i++) {
                schemaFields.add(openLineage.newSchemaDatasetFacetFieldsBuilder().name(outputSchemaNames[i]).build());
            }
        }

        OpenLineage.SchemaDatasetFacet schemaFacet = openLineage.newSchemaDatasetFacetBuilder().fields(schemaFields).build();
        this.outputDatasets = Arrays.asList(
                openLineage.newOutputDatasetBuilder().name(outputName).namespace(outputNamespace)
                        .facets(
                                openLineage.newDatasetFacetsBuilder().schema(schemaFacet).build()
                        ).build()
        );
        return this;
    }


    @Override
    public void onJobSubmitted(@Nullable JobClient jobClient, @Nullable Throwable throwable) {
        log.info("------lake onjobsubmit----jobid:{}",jobClient.getJobID());

        OpenLineage.RunEvent runStateUpdate =
                openLineage.newRunEventBuilder()
                        .eventType(OpenLineage.RunEvent.EventType.RUNNING)
                        .eventTime(ZonedDateTime.now())
                        .run(this.run)
                        .job(this.job)
                        .inputs(this.inputDatasets)
                        .outputs(this.outputDatasets)
                        .build();
        if(this.inputDatasets != null || this.outputDatasets != null) {
            this.client.emit(runStateUpdate);
        }
    }

    @Override
    public void onJobExecuted(@Nullable JobExecutionResult jobExecutionResult, @Nullable Throwable throwable) {
        if (jobExecutionResult instanceof DetachedJobExecutionResult) {
            log.warn("Job running in detached mode. Set execution.attached to true if you want to emit completed events.");
        } else {
            OpenLineage.RunEvent runStateUpdate = null;
            if (jobExecutionResult != null) {
                log.info("------onjobexecuted----jobresult:{}",jobExecutionResult.getJobExecutionResult().toString());
                runStateUpdate =
                        openLineage.newRunEventBuilder()
                                .eventType(OpenLineage.RunEvent.EventType.COMPLETE)
                                .eventTime(ZonedDateTime.now())
                                .run(this.run)
                                .job(this.job)
                                .inputs(this.inputDatasets)
                                .outputs(this.outputDatasets)
                                .build();
            } else {
                log.info("------onjobexecuted----jobresult:{null}");
                OpenLineage.Run failRun = openLineage.newRunBuilder().runId(this.runId).facets(openLineage.newRunFacetsBuilder().errorMessage(openLineage.newErrorMessageRunFacet(throwable.getMessage(), "JAVA", ExceptionUtils.getStackTrace(throwable))).build()).build();
                runStateUpdate =
                        openLineage.newRunEventBuilder()
                                .eventType(OpenLineage.RunEvent.EventType.FAIL)
                                .eventTime(ZonedDateTime.now())
                                .run(failRun)
                                .job(this.job)
                                .inputs(this.inputDatasets)
                                .outputs(this.outputDatasets)
                                .build();
            }
            this.client.emit(runStateUpdate);
        }
    }

}
