from pathlib import Path
from typing import Union, Optional
from urllib.parse import quote_plus
import requests
from urllib3.util.retry import Retry
from requests.adapters import HTTPAdapter
from pydantic import BaseModel


class WikidataSupplierConfig(BaseModel):
    endpoint_url: str = "https://query.wikidata.org/sparql"
    backoff_factor: float = 0.5
    max_retries: int = 5
    connect_timeout: float = 3
    read_timeout: float = 15

class RequestWikidata:
    """
    A class to send a SPARQL request to Wikidata and save the result to a file.
    """
    headers = {
        "Content-type": "application/x-www-form-urlencoded",
        "Accept": "text/csv"
    }

    def __init__(
            self,
            output_path: Union[str, Path],
            request: Union[str, Path],
            endpoint_url: str = "https://query.wikidata.org/sparql",
            backoff_factor: float = 0.5,
            max_retries: int = 5,
            timeout: tuple[float, float] = (3, 15)
        ):
        self.output_path = output_path
        self.request = request
        self.endpoint_url = endpoint_url
        self.backoff_factor = backoff_factor
        self.max_retries = max_retries
        self.timeout = timeout

    def get_endpoint_url(self) -> str:
        return self.endpoint_url

    def send(self):
        request_str : Optional[str] = None

        if isinstance(self.request, Path) or isinstance(self.request, str):
            try:
                with open(self.request, 'r', encoding='utf-8') as file:
                    request_str = file.read()
            except Exception as e:
                raise RuntimeError(f"Failed to read file {self.request}") from e
        else:
            if Path(self.request).exists():
                try:
                    with open(self.request, 'r', encoding='utf-8') as file:
                        request_str = file.read()
                except Exception as e:
                    raise RuntimeError(f"Failed to read file {self.request}") from e
            else:
                request_str = self.request

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
            with session.post(
                url=self.get_endpoint_url(),
                data="format=text/csv&query="+quote_plus(request_str),
                headers=RequestWikidata.headers,
                timeout=self.timeout
            ) as response:
                if response.status_code != 200:
                    raise requests.exceptions.HTTPError(f"HTTP error while querying Wikidata: {response.status_code}") from e
                
                if isinstance(output_path, str):
                    output_path = Path(output_path)
                if not output_path.parent.exists():
                    output_path.parent.mkdir(parents=True, exist_ok=True)
                if output_path.exists():
                    output_path.unlink()

                with open(output_path, "wb") as foutput:
                    for chunk in response.iter_content(chunk_size=8192):
                        if chunk:
                            foutput.write(chunk)

        except requests.exceptions.Timeout as e:
            raise TimeoutError(f"Timeout occurred while querying Wikidata") from e
        except requests.exceptions.ConnectionError as e:
            raise ConnectionError(f"Connection error while querying Wikidata") from e
        except requests.exceptions.HTTPError as e:
            raise e
        except requests.exceptions.RequestException  as e:
            raise RuntimeError(f"Request error while querying Wikidata") from e
        except Exception as e:
            raise RuntimeError(f"Unexpected error while querying Wikidata") from e
