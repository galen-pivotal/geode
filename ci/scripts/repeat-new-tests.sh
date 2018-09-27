#!/usr/bin/env bash

#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

set -e

SOURCE="${BASH_SOURCE[0]}"
while [[ -h "$SOURCE" ]]; do # resolve $SOURCE until the file is no longer a symlink
  SCRIPTDIR="$( cd -P "$( dirname "$SOURCE" )" && pwd )"
  SOURCE="$(readlink "$SOURCE")"
  [[ $SOURCE != /* ]] && SOURCE="$DIR/$SOURCE" # if $SOURCE was a relative symlink, we need to resolve it relative to the path where the symlink file was located
done
SCRIPTDIR="$( cd -P "$( dirname "$SOURCE" )" && pwd )"

function changes_for_path() {
  pushd geode
  local path="$1" # only expand once in the line below
  git git diff --name-only HEAD $(git merge-base HEAD origin/develop) -- $path
  popd
}

UNIT_TEST_CHANGES=$(changes_for_path('*/src/test/java'))
INTEGRATION_TEST_CHANGES=$(changes_for_path('*/src/integrationTest/java'))
DISTRIBUTED_TEST_CHANGES=$(changes_for_path('*/src/distributedTest/java'))
ACCEPTANCE_TEST_CHANGES=$(changes_for_path('*/src/acceptanceTest/java'))
UPGRADE_TEST_CHANGES=$(changes_for_path('*/src/upgradeTest/java'))

CHANGED_FILES_ARRAY=( $UNIT_TEST_CHANGES $INTEGRATION_TEST_CHANGES $DISTRIBUTED_TEST_CHANGES $ACCEPTANCE_TEST_CHANGES $UPGRADE_TEST_CHANGES )
NUM_CHANGED_FILES=${#CHANGED_FILES_ARRAY[@]}

TESTS_FLAG=""

echo "${NUM_CHANGED_FILES} changed tests"

if [[  "${NUM_CHANGED_FILES}" -eq 0 ]]
  then
    echo "No changed test files, nothing to test."
    exit 0
fi

if [[ "${NUM_CHANGED_FILES}" -gt 25 ]]
  then
    echo "${NUM_CHANGED_FILES} is too many changed tests to stress test. Allowing this job to pass without stress testing."
    exit 0
fi

TEST_TARGETS=""

if [[ "$UNIT_TEST_CHANGES" == "" ]]
  then
    TEST_TARGETS="$TEST_TARGETS test"
fi

if [[ "$DISTRIBUTED_TEST_CHANGES" == "" ]]
  then
    TEST_TARGETS="$TEST_TARGETS repeatDistributedTest"
fi

if [[ "$INTEGRATION_TEST_CHANGES" == "" ]]
  then
    TEST_TARGETS="$TEST_TARGETS repeatIntegrationTest"
fi

if [[ "$ACCEPTANCE_TEST_CHANGES" == "" ]]
  then
    TEST_TARGETS="$TEST_TARGETS repeatAcceptanceTest"
fi

if [[ "$UPGRADE_TEST_CHANGES" == "" ]]
  then
    TEST_TARGETS="$TEST_TARGETS repeatUpgradeTest"
fi

for FILENAME in $CHANGED_FILES ; do
  SHORT_NAME=$(basename $FILENAME)
  SHORT_NAME="${SHORT_NAME%.java}"
  TESTS_FLAG="$TESTS_FLAG --tests $SHORT_NAME"
done

export GRADLE_TASK='compileTestJava compileIntegrationTestJava compileDistributedTestJava $TEST_TARGETS'
export GRADLE_TASK_OPTIONS="--no-parallel -Prepeat=50 $TESTS_FLAG"

echo "GRADLE_TASK_OPTIONS=${GRADLE_TASK_OPTIONS}"

${SCRIPTDIR}/execute_tests.sh

