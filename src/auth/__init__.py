"""
Authentication package

Modules for handling authentication challenges, like MFA and CAPTCHA
"""

from src.auth.mfa_handler import MFAHandler
from src.auth.captcha_solver import CaptchaSolver
from src.auth.otp_reader import OTPReader

__all__ = [
    'MFAHandler',
    'CaptchaSolver',
    'OTPReader',
]
