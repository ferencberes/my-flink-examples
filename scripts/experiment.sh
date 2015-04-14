#!/bin/bash -eu
date=$(date +%Y-%m-%d-%H-%M-%S)

thisDir="$(dirname $0)"
thisDir="$(readlink -f "$thisDir")"
jarPath="$thisDir"/../target/my-flink-examples-0.1-jar-with-dependencies.jar 
classPath=hu.sztaki.ilab.recommender_global_toplist_extractor.GlobalTopListExtractor

feature_num="$1"
user_num="$2"
item_num="$3"
top_k="$4"

pushd "$thisDir"

"$FLINK_HOME"/bin/flink run --verbose --class "$classPath" "$jarPath" "$feature_num" "$user_num" "$item_num" "$top_k"

popd

