cd ~/looker-swap
pbpaste > run_migration.py   # copy from artifact first
git add run_migration.py
git commit -m "add check_explore, fix validate"
git push