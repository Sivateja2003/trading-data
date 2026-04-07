import os
import json
import datetime
from kiteconnect import KiteConnect
from dotenv import load_dotenv
from logger import logger

# Load environment variables
load_dotenv()

TOKEN_FILE = ".token.json"

class KiteAuthenticator:
    def __init__(self):
        self.api_key = os.getenv("KITE_API_KEY")
        self.api_secret = os.getenv("KITE_API_SECRET")
        self.kite = KiteConnect(api_key=self.api_key)
        
    def get_login_url(self):
        """Returns the Zerodha login URL."""
        return self.kite.login_url()

    def _save_token(self, access_token):
        """Saves access token locally."""
        data = {
            "access_token": access_token,
            "created_at": str(datetime.date.today())
        }
        with open(TOKEN_FILE, "w") as f:
            json.dump(data, f)
        logger.info("Access token saved locally.")

    def _load_token(self):
        """Loads access token if it exists and is for today."""
        if not os.path.exists(TOKEN_FILE):
            return None
            
        with open(TOKEN_FILE, "r") as f:
            data = json.load(f)
            
        # Zerodha tokens expire daily
        if data.get("created_at") == str(datetime.date.today()):
            return data.get("access_token")
        
        logger.info("Stored token is expired.")
        return None

    def authenticate(self):
        """Authenticates and returns the kite object."""
        access_token = self._load_token()
        
        if access_token:
            try:
                self.kite.set_access_token(access_token)
                self.access_token = access_token
                # Verify token by fetching profile
                self.kite.profile()
                logger.info("Successfully authenticated using stored token.")
                return self.kite
            except Exception as e:
                logger.warning(f"Stored token invalid: {e}. Re-authenticating...")

        # Fallback: Manual login
        print(f"Login here: {self.get_login_url()}")
        request_token = input("Enter request_token from the URL: ")
        
        try:
            data = self.kite.generate_session(request_token, api_secret=self.api_secret)
            access_token = data["access_token"]
            self.kite.set_access_token(access_token)
            self.access_token = access_token
            self._save_token(access_token)
            logger.info("Successfully authenticated manually.")
            return self.kite
        except Exception as e:
            logger.error(f"Authentication failed: {e}")
            raise

if __name__ == "__main__":
    # Test authentication
    auth = KiteAuthenticator()
    try:
        kite = auth.authenticate()
        print("Profile:", kite.profile())
    except Exception as e:
        print("Error:", e)
