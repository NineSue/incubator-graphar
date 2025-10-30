#!/bin/bash

# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

set -eu

cur_dir="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
jar_file="${cur_dir}/../snb-graphar-bridge/target/snb-graphar-bridge-0.13.0-SNAPSHOT.jar"
ldbc_jar="${cur_dir}/../ldbc-snb-datagen/target/ldbc_snb_datagen_2.12_spark3.2-0.5.1+23-1d60a657-jar-with-dependencies.jar"
output_dir="/tmp/graphar/snb"

# Parameters: scale_factor (default 0.003)
scale_factor="${1:-0.003}"

echo "Running snb-graphar-bridge with SF=${scale_factor}"
echo "Output directory: ${output_dir}"

# Memory configuration for different scale factors
if (( $(echo "$scale_factor <= 0.003" | bc -l) )); then
    driver_memory="2g"
    executor_memory="2g"
elif (( $(echo "$scale_factor <= 0.1" | bc -l) )); then
    driver_memory="4g"
    executor_memory="4g"
else
    driver_memory="8g"
    executor_memory="8g"
fi

echo "Memory configuration: Driver=${driver_memory}, Executor=${executor_memory}"

spark-submit \
  --class org.apache.graphar.datasources.ldbc.examples.LdbcEnhancedBridgeExample \
  --driver-memory "${driver_memory}" \
  --executor-memory "${executor_memory}" \
  --jars "${ldbc_jar}" \
  "${jar_file}" \
  "${scale_factor}" "${output_dir}" ldbc_snb 256 256 parquet

echo "Conversion completed successfully!"
echo "Output files:"
ls -lh "${output_dir}/"
