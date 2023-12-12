#!/bin/bash

script_path=$(dirname $0)
cd $script_path
repo_path=$(git rev-parse --show-toplevel)
cd $repo_path
eval $(ssh-agent)
ssh-add ~/.ssh/git_write

git add --all
git commit -m update
git push
