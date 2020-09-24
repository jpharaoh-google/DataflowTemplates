/*
 * Copyright (C) 2018 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package com.google.jpharaoh;

import com.google.cloud.teleport.templates.common.DatastoreConverters;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.ValueProvider;

/**
 * Dataflow template which deletes pulled Datastore Entities.
 */
public class DatastoreDelete {

  /**
   * Custom PipelineOptions.
   */
  public interface DatastoreDeleteOptions extends PipelineOptions {

    @Description("Project ID to delete entities from")
    ValueProvider<String> getDatastoreDeleteProjectId();
    void setDatastoreDeleteProjectId(ValueProvider<String> datastoreDeleteProjectId);

    @Description("Namespace to delete entities from")
    ValueProvider<String> getDatastoreDeleteNamespace();
    void setDatastoreDeleteNamespace(ValueProvider<String> datastoreDeleteNamespace);

    @Description("Query to find entities to delete")
    ValueProvider<String> getDatastoreDeleteQuery();
    void setDatastoreDeleteQuery(ValueProvider<String> datastoreDeleteQuery);

  }

  /**
   * Runs a pipeline which reads in Entities from datastore, passes in the JSON encoded Entities
   * to a Javascript UDF, and deletes all the Entities.
   *
   * <p>If the UDF returns value of undefined or null for a given Entity, then that Entity will not
   * be deleted.
   *
   * @param args arguments to the pipeline
   */
  public static void main(String[] args) {
    DatastoreDeleteOptions options = PipelineOptionsFactory.fromArgs(args)
        .withValidation()
        .as(DatastoreDeleteOptions.class);

    Pipeline pipeline = Pipeline.create(options);

    pipeline
        .apply(DatastoreConverters.ReadJsonEntities.newBuilder()
            .setGqlQuery(options.getDatastoreDeleteQuery())
            .setProjectId(options.getDatastoreDeleteProjectId())
            .setNamespace(options.getDatastoreDeleteNamespace())
            .build())
        .apply(DatastoreConverters.DatastoreDeleteEntityJson.newBuilder()
            .setProjectId(options.getDatastoreDeleteProjectId())
            .build());

    pipeline.run();
  }

}
