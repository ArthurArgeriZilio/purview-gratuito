from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobServiceClient, ContainerClient
from azure.core.exceptions import ResourceNotFoundError
import logging

class StorageCollector:
    """
    Collector for Azure Blob Storage and Data Lake Storage Gen2.
    Scans containers, blobs, and extracts metadata (size, type, last modified).
    """
    
    def __init__(self):
        self.credential = DefaultAzureCredential()
        self.logger = logging.getLogger(__name__)
    
    def scan_storage_account(self, account_name: str) -> dict:
        """
        Scan a single storage account for containers and blobs.
        
        Args:
            account_name: Storage account name (e.g., 'mystorageacct')
        
        Returns:
            dict with containers and blobs metadata
        """
        account_url = f"https://{account_name}.blob.core.windows.net"
        
        try:
            blob_service_client = BlobServiceClient(
                account_url=account_url,
                credential=self.credential
            )
            
            containers = []
            total_blobs = 0
            
            # List all containers
            for container in blob_service_client.list_containers():
                container_info = {
                    'name': container['name'],
                    'last_modified': container.get('last_modified'),
                    'public_access': container.get('public_access'),
                    'blobs': []
                }
                
                # List blobs in container (limit to 1000 per container)
                container_client = blob_service_client.get_container_client(container['name'])
                blob_count = 0
                
                try:
                    for blob in container_client.list_blobs():
                        if blob_count >= 1000:  # Limit for performance
                            break
                        
                        blob_info = {
                            'name': blob.name,
                            'size': blob.size,
                            'content_type': blob.get('content_settings', {}).get('content_type'),
                            'last_modified': blob.last_modified,
                            'tier': blob.blob_tier,
                            'etag': blob.etag
                        }
                        
                        # Detect file type from extension
                        if '.' in blob.name:
                            extension = blob.name.rsplit('.', 1)[1].lower()
                            blob_info['file_type'] = extension
                        
                        container_info['blobs'].append(blob_info)
                        blob_count += 1
                        total_blobs += 1
                    
                    container_info['blob_count'] = blob_count
                    
                except Exception as e:
                    self.logger.warning(f"Error listing blobs in container {container['name']}: {e}")
                    container_info['blob_count'] = 0
                    container_info['error'] = str(e)
                
                containers.append(container_info)
            
            return {
                'account_name': account_name,
                'account_url': account_url,
                'containers': containers,
                'total_containers': len(containers),
                'total_blobs_scanned': total_blobs
            }
        
        except Exception as e:
            self.logger.error(f"Failed to scan storage account {account_name}: {e}")
            return {
                'account_name': account_name,
                'error': str(e),
                'containers': []
            }
    
    def detect_data_formats(self, blob_name: str) -> dict:
        """
        Detect data format types for classification.
        
        Returns:
            dict with format classification
        """
        format_map = {
            'parquet': 'Structured',
            'csv': 'Structured',
            'json': 'Semi-Structured',
            'avro': 'Structured',
            'orc': 'Structured',
            'txt': 'Unstructured',
            'log': 'Unstructured',
            'xml': 'Semi-Structured',
            'xlsx': 'Structured',
            'pdf': 'Unstructured',
            'png': 'Media',
            'jpg': 'Media',
            'jpeg': 'Media',
            'mp4': 'Media',
            'zip': 'Archive'
        }
        
        if '.' in blob_name:
            ext = blob_name.rsplit('.', 1)[1].lower()
            return {
                'extension': ext,
                'category': format_map.get(ext, 'Unknown')
            }
        
        return {'extension': None, 'category': 'Unknown'}
