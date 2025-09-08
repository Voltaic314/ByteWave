# How to run a traversal test

## (Note: This is only available on Windows environments for now)

1. run this script by copying and pasting this into your terminal: 
"code\_dev_env_setup\setup_windows_env.ps1" (without quotes). 
Please note you may need admin permissions to run this or at least elevated 
permissions beyond typical read / write file permissions.

2. Once that is done, head over to tests/traversal_tests/config.json to modify 
the config file if necessary to tweak your test generation

3. Then run this script: "tests\bootstrap_test_run.ps1" (again without quotes) 
and then from there it should run and when it finishes you'll see print outs 
in the terminal saying it has completed. If the test runs for longer than it feels 
like it should, then just kill it and call it failed. 
