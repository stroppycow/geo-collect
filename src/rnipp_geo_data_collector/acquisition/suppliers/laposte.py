from pathlib import Path
from typing import Union, Optional
from urllib.parse import quote_plus
import requests
from urllib3.util.retry import Retry
from requests.adapters import HTTPAdapter
from pydantic import BaseModel


class LaPosteSupplierConfig(BaseModel):
    endpoint_url: str = "https://datanova.laposte.fr/data-fair/api/v1/datasets/laposte-hexasmal/raw"
    backoff_factor: float = 0.5
    max_retries: int = 5
    connect_timeout: float = 3
    read_timeout: float = 15

class RequestLaPosteHexasmal:
    """Class to load hexasmal base of La Poste"""

    def __init__(
            self,
            output_path: Union[str, Path],
            endpoint_url: str = "https://datanova.laposte.fr/data-fair/api/v1/datasets/laposte-hexasmal/raw",
            backoff_factor: float = 0.5,
            max_retries: int = 5,
            timeout: tuple[float, float] = (3, 15)
        ):
        self.output_path = output_path
        self.endpoint_url = endpoint_url
        self.backoff_factor = backoff_factor
        self.max_retries = max_retries
        self.timeout = timeout

    def get_endpoint_url(self) -> str:
        return self.endpoint_url

    def send(self):
        try:
            retry_strategy = Retry(
                total=self.max_retries,
                backoff_factor=self.backoff_factor,
                status_forcelist=[408, 429, 500, 502, 503, 504],
                redirect=0
            )
            adapter = HTTPAdapter(max_retries=retry_strategy)
            session = requests.Session()
            session.mount("https://", adapter)
            session.mount("http://", adapter)
            with session.get(
                url=self.get_endpoint_url(),
                data=None,
                timeout=self.timeout
            ) as response:
                if response.status_code != 200:
                    raise requests.exceptions.HTTPError(f"HTTP error while querying La Poste: {response.status_code}") from e
                
                if isinstance(self.output_path, str):
                    self.output_path = Path(self.output_path)
                if not self.output_path.parent.exists():
                    self.output_path.parent.mkdir(parents=True, exist_ok=True)

                if self.output_path.exists():
                    self.output_path.unlink()
                
                with open(self.output_path, "wb") as foutput:
                    for chunk in response.iter_content(chunk_size=8192):
                        if chunk:
                            foutput.write(chunk)

        except requests.exceptions.Timeout as e:
            raise TimeoutError(f"Timeout occurred while querying La Poste") from e
        except requests.exceptions.ConnectionError as e:
            raise ConnectionError(f"Connection error while querying La Poste") from e
        except requests.exceptions.HTTPError as e:
            raise e
        except requests.exceptions.RequestException  as e:
            raise RuntimeError(f"Request error while querying La Poste") from e
        except Exception as e:
            raise RuntimeError(f"Unexpected error while querying La Poste") from e
