import os
import yaml
import requests
import zipfile
import geopandas as gpd
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse
import signal
import shutil
import sys

class openinspire:
    def __init__(self, config_path):
        if not os.path.exists(config_path):
            raise FileNotFoundError(f"Config file not found: {config_path}")
            
        with open(config_path, 'r') as f:
            self.config = yaml.safe_load(f)
        
        self.base_url = self.config.get('source')
        self.cache_dir = self.config.get('cache_dir', './cache')
        self.extract_dir = os.path.join(self.cache_dir, "gml_temp")
        self.target_crs = "EPSG:27700"
        
        config_base = os.path.splitext(os.path.basename(config_path))[0]
        raw_output = self.config.get('output', 'inspire.gpkg')
        self.output_gpkg = raw_output.replace('[SCRIPTNAME]', config_base)
        
        os.makedirs(self.cache_dir, exist_ok=True)
        if os.path.exists(self.extract_dir):
            shutil.rmtree(self.extract_dir)
        os.makedirs(self.extract_dir, exist_ok=True)

    def log(self, message):
        print(f"[openinspire] {message}", flush=True)

    def _get_links(self):
        self.log(f"Scraping source: {self.base_url}")
        try:
            headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)'}
            r = requests.get(self.base_url, headers=headers, timeout=30)
            soup = BeautifulSoup(r.text, 'html.parser')
            
            links = []
            for a in soup.find_all('a', href=True):
                href = a['href']
                text = a.get_text(strip=True).lower()
                if href.endswith('.zip') and (".gml" in text or "land" in text):
                    links.append(urljoin(self.base_url, href))
            
            return sorted(list(set(links)))
        except Exception as e:
            self.log(f"Failed to scrape links: {e}")
            return []

    def _download_file(self, url):
        filename = os.path.basename(urlparse(url).path)
        zip_path = os.path.join(self.cache_dir, filename)

        if not os.path.exists(zip_path):
            try:
                r = requests.get(url, timeout=60, stream=True)
                r.raise_for_status()
                with open(zip_path, 'wb') as f:
                    for chunk in r.iter_content(chunk_size=8192):
                        f.write(chunk)
            except Exception as e:
                self.log(f"Download failed for {filename}: {e}")
                return False
        return True

    def _unzip_all(self):
        zip_files = [os.path.join(self.cache_dir, f) for f in os.listdir(self.cache_dir) if f.endswith('.zip')]
        total_zips = len(zip_files)
        
        for index, zip_path in enumerate(zip_files, 1):
            zip_name_no_ext = os.path.splitext(os.path.basename(zip_path))[0]
            self.log(f"[{index}/{total_zips}] Renaming & Extracting {zip_name_no_ext}...")
            
            try:
                with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                    gml_members = [m for m in zip_ref.namelist() if m.endswith('.gml')]
                    for member in gml_members:
                        unique_gml_name = f"{zip_name_no_ext}_{os.path.basename(member)}"
                        target_path = os.path.join(self.extract_dir, unique_gml_name)
                        with zip_ref.open(member) as source, open(target_path, "wb") as target:
                            shutil.copyfileobj(source, target)
            except Exception as e:
                self.log(f"Failed to process {zip_path}: {e}")

    def _amalgamate_gmls(self):
        gml_files = [os.path.join(self.extract_dir, f) for f in os.listdir(self.extract_dir) if f.endswith('.gml')]
        if not gml_files:
            self.log("No GML files found.")
            return

        total_gmls = len(gml_files)
        is_first = True
        
        if os.path.exists(self.output_gpkg):
            os.remove(self.output_gpkg)

        for index, gml_path in enumerate(gml_files, 1):
            self.log(f"[{index}/{total_gmls}] Consolidating {os.path.basename(gml_path)}...")
            try:
                gdf = gpd.read_file(gml_path, engine='pyogrio', use_arrow=True)
                if not gdf.empty:
                    if gdf.crs != self.target_crs:
                        gdf = gdf.to_crs(self.target_crs)
                    mode = 'w' if is_first else 'a'
                    gdf.to_file(self.output_gpkg, driver="GPKG", engine='pyogrio', mode=mode)
                    is_first = False
            except Exception as e:
                self.log(f"Error merging {os.path.basename(gml_path)}: {e}")

    def run(self):
        signal.signal(signal.SIGINT, lambda sig, frame: os._exit(0))
        self.log(f"Output: {self.output_gpkg}")
        
        links = self._get_links()
        if not links:
            self.log("No links found.")
            return

        self.log("--- Phase 1: Downloading ---")
        for url in links:
            self._download_file(url)

        self.log("--- Phase 2: Unzipping ---")
        self._unzip_all()

        self.log("--- Phase 3: Amalgamating ---")
        self._amalgamate_gmls()
        
        shutil.rmtree(self.extract_dir)
        self.log("Success: Process complete.")

def main():
    import sys
    import os
    import importlib.resources as pkg_resources

    # Check if the user provided a path
    if len(sys.argv) >= 2:
        config_path = sys.argv[1]
    else:
        # Fallback: Use the internal inspire.yml bundled with the package
        print("[openinspire] No config provided. Using default internal inspire.yml...")
        
        # This finds the path to the yml file inside your installed package
        try:
            # For Python 3.9+
            with pkg_resources.as_file(pkg_resources.files('openinspire').joinpath('inspire.yml')) as p:
                config_path = str(p)
        except Exception:
            import openinspire
            config_path = os.path.join(os.path.dirname(openinspire.__file__), 'inspire.yml')

    if not os.path.exists(config_path):
        print(f"Error: Could not find config at {config_path}")
        sys.exit(1)

    app = openinspire(config_path)
    app.run()