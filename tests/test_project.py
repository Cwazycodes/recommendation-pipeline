from realtime_recommendation_pipeline import __name__


def test_project_package_name() -> None:
    assert __name__ == "realtime_recommendation_pipeline"
