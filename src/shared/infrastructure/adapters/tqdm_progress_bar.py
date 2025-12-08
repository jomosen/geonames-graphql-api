from typing import Iterable, Callable, Any
from tqdm import tqdm
from shared.application.ports.progress_bar_port import ProgressBarPort


class TqdmProgressBar(ProgressBarPort):
    """
    TQDM-based progress bar that can be fully customized.
    Accepts all tqdm arguments via **kwargs.
    """

    def __init__(self, *, total: int, desc: str, **tqdm_kwargs):
        """
        Args:
            total: Required total number of iterations.
            desc: Required description for the progress bar.
            **tqdm_kwargs: Any valid argument for tqdm (e.g., unit, colour, smoothing, etc.).
        """
        self.total = total
        self.desc = desc
        self.tqdm_kwargs = tqdm_kwargs
        self._bar = None

    def __enter__(self):
        self._bar = tqdm(total=self.total, desc=self.desc, **self.tqdm_kwargs)
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        if self._bar:
            # Ensure bar finishes correctly
            if self._bar.n < self.total:
                self._bar.n = self.total
                self._bar.refresh()
            self._bar.close()

    def update(self, step: int = 1):
        if self._bar:
            self._bar.update(step)

    def write(self, message: str):
        if self._bar:
            self._bar.write(message)

    def run(self, iterable: Iterable[int]):
        """
        Runs the progress bar for an iterable that yields step counts (e.g. batch sizes).
        Each yielded value represents the number of processed records to increment.
        """
        for step in iterable:
            self.update(step)
