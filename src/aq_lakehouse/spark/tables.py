from aq_lakehouse.config import PipelineConfig


class Tables:
    def __init__(self, config: PipelineConfig) -> None:
        self.cfg = config

    # bronze
    def bronze_archive_sheets(self, year: int) -> str:
        return self.cfg.layer_uri("bronze", "gios_archive", "sheets", f"year={year}")

    def bronze_metadata(self, snapshot: str) -> str:
        return self.cfg.layer_uri("bronze", "gios_archive", "metadata", f"snapshot={snapshot}")

    # silver
    @property
    def stations(self) -> str:
        return self.cfg.layer_uri("silver", "stations")

    @property
    def station_code_map(self) -> str:
        return self.cfg.layer_uri("silver", "station_code_map")

    @property
    def sensors(self) -> str:
        return self.cfg.layer_uri("silver", "sensors")

    @property
    def measurements(self) -> str:
        return self.cfg.layer_uri("silver", "measurements")
