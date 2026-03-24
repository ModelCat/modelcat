import os
import tempfile
import unittest
from PIL import Image
from modelcat.connector.validate import DatasetValidator


class TestThumbnailGeneration(unittest.TestCase):
    def test_thumbnail_scenarios(self):
        # Scenarios: (original_width, original_height, expected_width, expected_height)
        scenarios = [
            (1000, 500, 260, 130),    # Normal Wide (aspect ratio 2:1)
            (1500, 500, 260, 260),    # Extremely Wide (aspect ratio 3:1) - Square crop
            (500, 1000, 260, 520),    # Normal Tall (aspect ratio 1:2)
            (500, 1500, 260, 260),    # Extremely Tall (aspect ratio 1:3) - Square crop
            (100, 100, 100, 100),     # Small (No resizing)
            (260, 260, 260, 260),     # Exact max width size
            (200, 500, 200, 500),     # Narrow but not extremely tall (height < 3x width)
        ]

        formats_to_test = [
            ("PNG", ".png"),
            ("JPEG", ".jpg"),
            ("BMP", ".bmp"),
            ("GIF", ".gif"),
            ("TIFF", ".tiff"),
            ("WEBP", ".webp")
        ]

        with tempfile.TemporaryDirectory() as tmpdir:
            dsv = DatasetValidator(tmpdir, tmpdir)

            for f_name, ext in formats_to_test:
                for w, h, ew, eh in scenarios:
                    with self.subTest(format=f_name, width=w, height=h):
                        dummy_path = os.path.join(tmpdir, f"test_{w}_{h}{ext}")
                        thumbnail_path = os.path.join(tmpdir, f"thumb_{w}_{h}_{f_name}.jpg")

                        # Create a dummy image in the specific format
                        img = Image.new("RGB", (w, h), color="purple")
                        img.save(dummy_path, format=f_name)

                        dsv.create_thumbnail(dummy_path, thumbnail_path, max_width=260)

                        self.assertTrue(os.path.exists(thumbnail_path))
                        with Image.open(thumbnail_path) as thumb:
                            # Output should always be JPEG
                            self.assertEqual(thumb.format, "JPEG")
                            self.assertEqual(thumb.size, (ew, eh))


if __name__ == '__main__':
    unittest.main()
