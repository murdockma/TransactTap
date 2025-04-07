"""
OTP Reader Module

Functionality to read OTP codes from sources, 
with a focus on macOS Messages app. Supports automated
reading and manual input fallbacks
"""

import os
import re
import time
import subprocess
from datetime import datetime, timedelta
from typing import Optional, List, Dict


class OTPReader:
    """
    Class for reading OTP codes from sources
    
    Provides ways to extract OTP codes from messages,
    primarily focusing on macOS Messages app integration with AppleScript.
    It handles automated extraction and fallback to manual input
    """
    
    def __init__(self):
        """Initialize OTP reader"""
        self.platform = self._detect_platform()
        self.last_check_time = datetime.now()
    
    def _detect_platform(self) -> str:
        """
        Detect the current operating system platform
        
        :return: String identifier for the platform ('macos', 'windows', 'linux')
        """
        if os.name == 'posix' and 'darwin' in os.uname().sysname.lower():
            return 'macos'
        elif os.name == 'nt':
            return 'windows'
        else:
            return 'linux'
    
    def get_latest_code(self, provider: str = None, timeout: int = 60, 
                       regex: str = r"(\d{6})", check_interval: int = 5) -> Optional[str]:
        """
        Get the latest OTP code
        
        :param provider: Name of the service provider (e.g., 'Chase', 'Wells Fargo')
        :param timeout: Maximum time to wait for an OTP in seconds
        :param regex: Regular expression pattern to extract the OTP
        :param check_interval: How often to check for new messages in seconds   
        :return: Extracted OTP code
        """
        if self.platform == 'macos':
            return self._get_otp_from_macos_messages(provider, timeout, regex, check_interval)
        else:
            return self._get_otp_manually(provider)
    
    def _get_otp_from_macos_messages(self, provider: Optional[str], timeout: int, 
                                     regex: str, check_interval: int) -> Optional[str]:
        """
        Get OTP code from macOS Messages using AppleScript
        
        :param provider: Name of the service provider
        :param timeout: Maximum time to wait for an OTP in seconds
        :param regex: Regular expression pattern to extract the OTP
        :param check_interval: How often to check for new messages
        :return: Extracted OTP code
        """
        start_time = datetime.now()
        end_time = start_time + timedelta(seconds=timeout)

        self.last_check_time = start_time
        
        while datetime.now() < end_time:
            try:
                # Read recent messages
                script = self._build_messages_applescript(provider)
                result = subprocess.run(
                    ['osascript', '-e', script], 
                    capture_output=True, 
                    text=True
                )
                
                if result.returncode != 0:
                    print(f"AppleScript error: {result.stderr}")
                    time.sleep(check_interval)
                    continue
                
                # Extract messages
                messages = result.stdout.strip().split('\n')
                
                # Look for OTP code in each message
                for message in messages:
                    if not message:
                        continue
                    
                    match = re.search(regex, message)
                    if match:
                        otp = match.group(1)
                        print(f"Found OTP code: {otp}")
                        return otp
                
                time.sleep(check_interval)
                
            except Exception as e:
                print(f"Error reading messages: {str(e)}")
                time.sleep(check_interval)
        
        print(f"OTP not found after waiting {timeout} seconds")
        return None
    
    def _build_messages_applescript(self, provider: Optional[str]) -> str:
        """
        Build AppleScript to extract recent messages
        
        :param provider: Optional provider name to filter messages
        :return: AppleScript code as string
        """
        since_time = self.last_check_time.strftime('%Y-%m-%d %H:%M:%S')
        
        # Get messages from last hour
        script = f"""
        tell application "Messages"
            set recentMessages to {{}}
            set oneHourAgo to (current date) - 3600
            
            repeat with chat in chats
                repeat with msg in (messages of chat whose date received > date "{since_time}")
                    set msgText to content of msg
        """
        
        # Add provider filter
        if provider:
            provider_escaped = provider.replace('"', '\\"')
            script += f"""
                    if msgText contains "{provider_escaped}" then
                        set end of recentMessages to msgText
                    end if
            """
        else:
            script += """
                    set end of recentMessages to msgText
            """
        
        script += """
                end repeat
            end repeat
            
            return recentMessages
        end tell
        """
        
        return script
    
    def _get_otp_manually(self, provider: Optional[str]) -> str:
        """
        Fallback to get OTP code from manual user input
        
        :param provider: Name of the provider
        :return: OTP code entered by user
        """
        provider_str = f" from {provider}" if provider else ""
        
        while True:
            otp = input(f"Please enter the OTP code{provider_str}: ").strip()
            if re.match(r'^\d{4,8}, otp'):
                return otp
            else:
                print("Invalid OTP format. Please enter a numeric code (usually 4-8 digits).")
    
    def extract_code_from_text(self, text: str, regex: str = r"(\d{6})") -> Optional[str]:
        """
        Extract OTP code from text
        
        :param text: Text to extract code from
        :param regex: Regular expression pattern to match
        :return: Extracted code
        """
        match = re.search(regex, text)
        return match.group(1) if match else None
