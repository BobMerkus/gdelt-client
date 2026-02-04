import pytest

from gdelt_client.validation import validate_tone


class TestValidateTone:
    def test_valid_tone_doesnt_raise_error(self):
        validate_tone(">5")

    def test_raises_when_comparator_missing(self):
        with pytest.raises(ValueError):
            validate_tone("10")

    def test_raises_when_equals_in_comparator(self):
        with pytest.raises(ValueError):
            validate_tone(">=10")

    def test_raises_when_multiple_tones(self):
        with pytest.raises(ValueError):
            validate_tone([">5", "<10"])
