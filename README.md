# Gobject

## Overview

Gobject is a object storage which I created to practice Go language.

## Features

### v0.1

- implement basic HTTP server with net/http package
  - support GET request to `/healthz` for health check
  - support POST request to `/objects` for generate id by sha256 hash of content
  - support GET request to `/objects/{id}` for retrieve object by id
