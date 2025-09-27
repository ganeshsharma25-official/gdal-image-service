import os
import numpy as np
from osgeo import gdal
import logging

logger = logging.getLogger(__name__)

class NDWIProcessor:
    
    def __init__(self):
        gdal.UseExceptions()
        self.green_band = 3
        self.nir_band = 8
    
    def process_ndwi(self, input_file_path, workspace, layer_name):
        """
        Process Sentinel-2 image to calculate NDWI and apply styling
        Returns output file path or None if processing fails
        """
        try:
            if not self._validate_input_file(input_file_path):
                return None
            
            ndwi_output_path = self._generate_output_path(input_file_path, layer_name, "_NDWI")
            styled_output_path = self._generate_output_path(input_file_path, layer_name, "_NDWI_styled")
            
            if os.path.exists(styled_output_path):
                logger.error(f"NDWI styled file already exists: {styled_output_path}")
                return None
            
            
            if not self._calculate_ndwi(input_file_path, ndwi_output_path):
                return None
            
           
            if not self._apply_ndwi_styling(ndwi_output_path, styled_output_path):
                self._cleanup_file(ndwi_output_path)
                return None
            
          
            self._cleanup_file(ndwi_output_path)
            
            logger.info(f"NDWI processing completed: {styled_output_path}")
            return styled_output_path
            
        except Exception as e:
            logger.error(f"NDWI processing failed: {str(e)}")
            return None
    
    def _validate_input_file(self, file_path):
        """Validate input file and required bands"""
        try:
            if not os.path.exists(file_path):
                logger.error(f"Input file not found: {file_path}")
                return False
            
            dataset = gdal.Open(file_path, gdal.GA_ReadOnly)
            if dataset is None:
                logger.error(f"Cannot open file with GDAL: {file_path}")
                return False
            
            band_count = dataset.RasterCount
            
            if band_count < max(self.green_band, self.nir_band):
                logger.error(f"Insufficient bands. Found {band_count}, need at least {max(self.green_band, self.nir_band)}")
                dataset = None
                return False
            
            green_band = dataset.GetRasterBand(self.green_band)
            nir_band = dataset.GetRasterBand(self.nir_band)
            
            if green_band is None or nir_band is None:
                logger.error("Required bands (3, 8) not accessible")
                dataset = None
                return False
            
            dataset = None
            return True
            
        except Exception as e:
            logger.error(f"File validation error: {str(e)}")
            return False
    
    def _generate_output_path(self, input_path, layer_name, suffix):
        """Generate output file path with suffix"""
        input_dir = os.path.dirname(input_path)
        output_filename = f"{layer_name}{suffix}.tif"
        return os.path.join(input_dir, output_filename)
    
    def _calculate_ndwi(self, input_path, output_path):
        """Calculate NDWI using GDAL operations"""
        input_ds = None
        output_ds = None
        
        try:
            input_ds = gdal.Open(input_path, gdal.GA_ReadOnly)
            
            green_band = input_ds.GetRasterBand(self.green_band)
            nir_band = input_ds.GetRasterBand(self.nir_band)
            
            cols = input_ds.RasterXSize
            rows = input_ds.RasterYSize
            
            green_data = green_band.ReadAsArray().astype(np.float32)
            nir_data = nir_band.ReadAsArray().astype(np.float32)
            
            green_nodata = green_band.GetNoDataValue()
            nir_nodata = nir_band.GetNoDataValue()
            
            ndwi = self._compute_ndwi_array(green_data, nir_data, green_nodata, nir_nodata)
            
            driver = gdal.GetDriverByName('GTiff')
            output_ds = driver.Create(
                output_path, cols, rows, 1, gdal.GDT_Float32,
                options=['COMPRESS=LZW', 'TILED=YES']
            )
            
            output_ds.SetGeoTransform(input_ds.GetGeoTransform())
            output_ds.SetProjection(input_ds.GetProjection())
            
            output_band = output_ds.GetRasterBand(1)
            output_band.WriteArray(ndwi)
            output_band.SetNoDataValue(-9999.0)
            output_band.FlushCache()
            
            return True
            
        except Exception as e:
            logger.error(f"NDWI calculation error: {str(e)}")
            return False
            
        finally:
            if input_ds:
                input_ds = None
            if output_ds:
                output_ds = None
    
    def _compute_ndwi_array(self, green, nir, green_nodata, nir_nodata):
        """Compute NDWI with proper nodata handling"""
        
        valid_mask = np.ones_like(green, dtype=bool)
        
        if green_nodata is not None:
            valid_mask &= (green != green_nodata)
        if nir_nodata is not None:
            valid_mask &= (nir != nir_nodata)
        
        valid_mask &= (green > 0) & (nir > 0)
        
        ndwi = np.full_like(green, -9999.0, dtype=np.float32)
        
        denominator = green + nir
        valid_calc = valid_mask & (denominator != 0)
        
        # NDWI = (Green - NIR) / (Green + NIR)
        ndwi[valid_calc] = (green[valid_calc] - nir[valid_calc]) / denominator[valid_calc]
        
        ndwi = np.clip(ndwi, -1.0, 1.0, out=ndwi, where=valid_calc)
        
        return ndwi
    
    def _apply_ndwi_styling(self, ndwi_path, output_rgb_path):
        """Apply NDWI color ramp to create RGB visualization"""
        try:
            ndwi_values = np.array([-1.0, -0.8, -0.3, 0.0, 0.1, 0.3, 0.5, 0.8, 1.0])
            
            colors_rgb = np.array([
                [0x00, 0x60, 0x00],  # Darker green at -1.0
                [0x00, 0x80, 0x00],  # Green at -0.8
                [0x60, 0xA0, 0x60],  # Light green at -0.3
                [0xFF, 0xFF, 0xFF],  # White at 0.0
                [0x40, 0x40, 0xB0],  # Very light blue at 0.1
                [0x40, 0x40, 0xB0],  # Light blue at 0.3
                [0x40, 0x40, 0xB0],  # Medium blue at 0.5
                [0x00, 0x00, 0xCC],  # Blue at 0.8
                [0x00, 0x00, 0xA0],  # Darker blue at 1.0
            ], dtype=np.uint8)
            
            ndwi_ds = gdal.Open(ndwi_path, gdal.GA_ReadOnly)
            ndwi_band = ndwi_ds.GetRasterBand(1)
            ndwi = ndwi_band.ReadAsArray()
            
            nodata = ndwi_band.GetNoDataValue()
            if nodata is not None:
                valid_mask = (ndwi != nodata)
            else:
                valid_mask = np.ones_like(ndwi, dtype=bool)
            
            ndwi_clipped = np.clip(ndwi, -1.0, 1.0)
            
            rows, cols = ndwi.shape
            red = np.zeros((rows, cols), dtype=np.uint8)
            green = np.zeros((rows, cols), dtype=np.uint8)
            blue = np.zeros((rows, cols), dtype=np.uint8)
            
            red_interp = np.interp(ndwi_clipped, ndwi_values, colors_rgb[:, 0])
            green_interp = np.interp(ndwi_clipped, ndwi_values, colors_rgb[:, 1])
            blue_interp = np.interp(ndwi_clipped, ndwi_values, colors_rgb[:, 2])
            
            red[valid_mask] = red_interp[valid_mask].astype(np.uint8)
            green[valid_mask] = green_interp[valid_mask].astype(np.uint8)
            blue[valid_mask] = blue_interp[valid_mask].astype(np.uint8)
            
            driver = gdal.GetDriverByName('GTiff')
            out_ds = driver.Create(output_rgb_path, cols, rows, 3, gdal.GDT_Byte,
                                  options=['COMPRESS=LZW', 'TILED=YES', 'PHOTOMETRIC=RGB'])
            
            out_ds.SetGeoTransform(ndwi_ds.GetGeoTransform())
            out_ds.SetProjection(ndwi_ds.GetProjection())
            
            out_ds.GetRasterBand(1).WriteArray(red)
            out_ds.GetRasterBand(2).WriteArray(green)
            out_ds.GetRasterBand(3).WriteArray(blue)
            
            out_ds.FlushCache()
            out_ds = None
            ndwi_ds = None
            
            return True
            
        except Exception as e:
            logger.error(f"NDWI styling error: {str(e)}")
            return False
    
    def _cleanup_file(self, file_path):
        """Remove file if it exists"""
        try:
            if os.path.exists(file_path):
                os.remove(file_path)
                logger.info(f"Cleaned up file: {file_path}")
        except Exception as e:
            logger.warning(f"Failed to cleanup file {file_path}: {str(e)}")
