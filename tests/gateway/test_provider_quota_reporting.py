import pytest
from gateway.run import _sanitize_gateway_final_response
from gateway.config import Platform

@pytest.mark.parametrize('detail',[
 'Codex provider quota exhausted (429); retry after 585192s. Credentials are still valid.',
 'usage limit reached',
])
def test_quota_wrapper_is_not_misreported_as_authentication(detail):
 text=_sanitize_gateway_final_response(Platform.TELEGRAM,'⚠️ Provider authentication failed: '+detail)
 assert 'usage limit' in text.lower()
 assert 'authentication failed' not in text.lower()
 assert 'wait a moment' not in text.lower()
 assert '585192' not in text

def test_transient_429_wrapper_is_rate_limit():
 text=_sanitize_gateway_final_response(Platform.TELEGRAM,'⚠️ Provider authentication failed: HTTP 429 too many requests')
 assert 'rate-limiting' in text
 assert 'authentication failed' not in text

def test_cron_quota_does_not_request_reauthentication(monkeypatch):
 from cron.scheduler import _preflight_check_provider_key
 from hermes_cli.auth import AuthError, CODEX_RATE_LIMITED_CODE
 import hermes_cli.runtime_provider as runtime
 def exhausted(**kwargs):
  raise AuthError('quota exhausted',provider='openai-codex',code=CODEX_RATE_LIMITED_CODE,relogin_required=False)
 monkeypatch.setattr(runtime,'resolve_runtime_provider',exhausted)
 result=_preflight_check_provider_key({'id':'fixture','provider':'openai-codex'}, {})
 assert 'usage limit' in result
 assert 'credential missing' not in result
 assert 'hermes setup' not in result
