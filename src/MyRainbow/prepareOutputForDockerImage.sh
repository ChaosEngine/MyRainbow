#!/bin/bash
#
docker run -it --rm -v $(pwd):/app --workdir /app microsoft/aspnetcore-build:latest bash -c "dotnet restore && dotnet publish -c Release"
