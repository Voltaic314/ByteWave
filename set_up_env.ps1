# set up the environment for the tests
$env:PATH += ";C:/msys64/mingw64/bin"
$env:PATH += ";C:/Users/golde/duckdb_c_lib"
$env:CGO_ENABLED = "1"
$env:CC = "x86_64-w64-mingw32-gcc"
$env:CGO_CFLAGS = "-IC:/Users/golde/duckdb_c_lib"
$env:CGO_LDFLAGS = "-LC:/Users/golde/duckdb_c_lib -lduckdb"
