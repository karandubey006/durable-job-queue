import os

import uvicorn

from fake_payments.app import app

uvicorn.run(app, host="0.0.0.0", port=int(os.environ.get("PAYMENTS_PORT", "8001")))
