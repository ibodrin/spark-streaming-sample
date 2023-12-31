#!/bin/bash

script_path=$(dirname $0)
cd $script_path
repo_path=$(git rev-parse --show-toplevel)
cd $repo_path
eval $(ssh-agent)
ssh-add ~/.ssh/git_read

git config --global user.email "spark@spark-test1"
git config --global user.name "spark"

git pull
