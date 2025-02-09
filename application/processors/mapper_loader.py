from importlib import import_module
import logging

logger = logging.getLogger(__name__)

def load_mappers(resources):
    """
    Dynamically load mappers for the given resources.
    """
    resource_mappers = {}
    for res in resources:
        try:
            # Dynamically import the mapper module and function
            mapper_module = import_module(f"fhir_mapping.{res['name'].lower()}_mapper")
            mapper_func = getattr(mapper_module, f"map_{res['name'].lower()}")
            resource_mappers[res["name"]] = mapper_func
        except ModuleNotFoundError:
            logger.warning(f"Mapper module not found for resource: {res['name']}")
        except AttributeError:
            logger.warning(f"Mapper function not found in module for resource: {res['name']}")
        except Exception as e:
            logger.error(f"Error loading mapper for resource {res['name']}: {e}")
    return resource_mappers
