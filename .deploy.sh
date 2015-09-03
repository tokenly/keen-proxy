#!/bin/bash

set -e

echo; echo "compiling assets"
npm install;
./node_modules/gulp/bin/gulp.js
