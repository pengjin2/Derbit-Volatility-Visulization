import hashlib, time, base64
import numpy as np
from scipy.stats import norm

access_key = ''
secret_key = ''

def get_signature(action, arguments):
    nonce = str(int(time.time() * 1000))

    signature_string = '_=%s&_ackey=%s&_acsec=%s&_action=%s' % (
        nonce, access_key, secret_key, action
    )

    for key, value in sorted(arguments.items()):
        if isinstance(value, list):
            value = "".join(str(v) for v in value)
        else:
            value = str(value)

        signature_string += "&%s=%s" % (key, value)

    sha256 = hashlib.sha256()
    sha256.update(signature_string.encode("utf-8"))
    signature_hash = base64.b64encode(sha256.digest()).decode()

    return "%s.%s.%s" % (access_key, nonce, signature_hash)


