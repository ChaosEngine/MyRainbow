#!/bin/bash
#
docker run -it --rm -v $(pwd):/app --workdir /app microsoft/aspnetcore-build:1.1.0-projectjson bash -c "dotnet restore && dotnet publish -c Release"
