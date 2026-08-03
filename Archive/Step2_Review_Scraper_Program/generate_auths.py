import os
from review_scraper.gui import ReviewScraperGUI

# A quick shim to login without the full GUI
class AuthGenerator(ReviewScraperGUI):
    def __init__(self):
        # Do NOT initialize Tkinter GUI
        pass
        
    def generate(self):
        for i in range(1, 4):
            auth_file = f"google_auth_{i}.json"
            print(f"--- Login {i}/3: {auth_file} ---")
            success = self._run_google_login(auth_file)
            if success:
                print(f"Successfully saved {auth_file}")
            else:
                print(f"Failed to save {auth_file}")

if __name__ == "__main__":
    generator = AuthGenerator()
    generator.generate()
