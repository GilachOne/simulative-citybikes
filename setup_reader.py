"""Run inside the project container; password is passed only through environment."""
import os
import re
from pipeline import ch

password=os.environ['CH_READER_PASSWORD']
if not re.fullmatch('[0-9a-f]{40}',password):
    raise ValueError('Generate credentials with init_env.py')
client=ch()
try:
    client.command(f"CREATE USER IF NOT EXISTS citybikes_reader IDENTIFIED WITH sha256_password BY '{password}'")
    client.command('GRANT SELECT ON citybikes.* TO citybikes_reader')
    client.command('ALTER USER citybikes_reader SETTINGS readonly=2, max_execution_time=30')
    print('Read-only dashboard account ready')
finally:client.close()
