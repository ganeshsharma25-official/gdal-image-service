import os
import numpy as np
from osgeo import gdal
import logging
import tempfile

logger = logging.getLogger(__name__)

class NDVIProcessor:
    
    def __init__(self):
        gdal.UseExceptions()
        self.red_band = 4
        self.nir_band = 8
    
    def process_ndvi(self, input_file_path, workspace, layer_name):
        """
        Process Sentinel-2 image to calculate NDVI
        Returns output file path or None if processing fails
        """
        try:
            if not self._validate_input_file(input_file_path):
                return None
            
            output_path = self._generate_output_path(input_file_path, layer_name)
            
            if os.path.exists(output_path):
                logger.error(f"NDVI file already exists: {output_path}")
                return None
            
            success = self._calculate_ndvi(input_file_path, output_path)
            
            if success:
                logger.info(f"NDVI processing completed: {output_path}")
                return output_path
            else:
                self._cleanup_failed_output(output_path)
                return None
                
        except Exception as e:
            logger.error(f"NDVI processing failed: {str(e)}")
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
            
            if band_count < max(self.red_band, self.nir_band):
                logger.error(f"Insufficient bands. Found {band_count}, need at least {max(self.red_band, self.nir_band)}")
                dataset = None
                return False
            
            red_band = dataset.GetRasterBand(self.red_band)
            nir_band = dataset.GetRasterBand(self.nir_band)
            
            if red_band is None or nir_band is None:
                logger.error("Required bands (4, 8) not accessible")
                dataset = None
                return False
            
            dataset = None
            return True
            
        except Exception as e:
            logger.error(f"File validation error: {str(e)}")
            return False
    
    def _generate_output_path(self, input_path, layer_name):
        """Generate output file path in same directory as input"""
        input_dir = os.path.dirname(input_path)
        output_filename = f"{layer_name}_NDVI.tif"
        return os.path.join(input_dir, output_filename)
    
    def _calculate_ndvi(self, input_path, output_path):
        """Calculate NDVI using GDAL operations"""
        input_ds = None
        output_ds = None
        
        try:
            input_ds = gdal.Open(input_path, gdal.GA_ReadOnly)
            
            red_band = input_ds.GetRasterBand(self.red_band)
            nir_band = input_ds.GetRasterBand(self.nir_band)
            
            cols = input_ds.RasterXSize
            rows = input_ds.RasterYSize
            
            red_data = red_band.ReadAsArray().astype(np.float32)
            nir_data = nir_band.ReadAsArray().astype(np.float32)
            
            red_nodata = red_band.GetNoDataValue()
            nir_nodata = nir_band.GetNoDataValue()
            
            ndvi = self._compute_ndvi_array(red_data, nir_data, red_nodata, nir_nodata)
            
            driver = gdal.GetDriverByName('GTiff')
            output_ds = driver.Create(
                output_path, cols, rows, 1, gdal.GDT_Float32,
                options=['COMPRESS=LZW', 'TILED=YES']
            )
            
            output_ds.SetGeoTransform(input_ds.GetGeoTransform())
            output_ds.SetProjection(input_ds.GetProjection())
            
            output_band = output_ds.GetRasterBand(1)
            output_band.WriteArray(ndvi)
            output_band.SetNoDataValue(-9999.0)
            output_band.FlushCache()
            
            return True
            
        except Exception as e:
            logger.error(f"NDVI calculation error: {str(e)}")
            return False
            
        finally:
            if input_ds:
                input_ds = None
            if output_ds:
                output_ds = None
    
    def _compute_ndvi_array(self, red, nir, red_nodata, nir_nodata):
        """Compute NDVI with proper nodata handling"""
        
        valid_mask = np.ones_like(red, dtype=bool)
        
        if red_nodata is not None:
            valid_mask &= (red != red_nodata)
        if nir_nodata is not None:
            valid_mask &= (nir != nir_nodata)
        
        valid_mask &= (red > 0) & (nir > 0)
        
        ndvi = np.full_like(red, -9999.0, dtype=np.float32)
        
        denominator = nir + red
        valid_calc = valid_mask & (denominator != 0)
        
        ndvi[valid_calc] = (nir[valid_calc] - red[valid_calc]) / denominator[valid_calc]
        
        ndvi = np.clip(ndvi, -1.0, 1.0, out=ndvi, where=valid_calc)
        
        return ndvi
    
    def _cleanup_failed_output(self, output_path):
        """Remove partially created output file on failure"""
        try:
            if os.path.exists(output_path):
                os.remove(output_path)
                logger.info(f"Cleaned up failed output: {output_path}")
        except Exception as e:
            logger.warning(f"Failed to cleanup output file: {str(e)}")
