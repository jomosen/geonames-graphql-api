import requests
from contextlib import nullcontext
from shared.infrastructure.adapters.exceptions.file_download_error import FileDownloadError
from shared.application.ports.progress_bar_port import ProgressBarPort
from shared.application.ports.file_downloader_port import FileDownloaderPort


class FileDownloader(FileDownloaderPort):
    def __init__(self, progress_bar_cls: type[ProgressBarPort] | None = None):
        self.progress_bar_cls = progress_bar_cls

    def download(self, url: str, dest_path: str) -> None:
        try:
            response = requests.get(url, stream=True)
            response.raise_for_status()

            total_size = int(response.headers.get("Content-Length", 0))
            chunk_size = 8192

            with open(dest_path, "wb") as f:
                if self.progress_bar_cls:
                    with self.progress_bar_cls(total=total_size, desc=f"Downloading {url}", unit="B") as bar:
                        for chunk in response.iter_content(chunk_size=chunk_size):
                            if chunk:
                                f.write(chunk)
                                bar.update(len(chunk))
                else:
                    for chunk in response.iter_content(chunk_size=chunk_size):
                        if chunk:
                            f.write(chunk)


        except Exception as e:
            raise FileDownloadError(f"Failed to download {url}: {e}") from e
