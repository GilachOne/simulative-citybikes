from pathlib import Path
import secrets

p = Path('.env')
if p.exists():
    raise SystemExit('.env already exists; keeping current credentials')
p.write_text('PG_PASSWORD='+secrets.token_hex(20)+'\nCH_PASSWORD='+secrets.token_hex(20)+'\nCH_READER_PASSWORD='+secrets.token_hex(20)+'\n')
p.chmod(0o600)
print('Created .env; do not publish it.')
