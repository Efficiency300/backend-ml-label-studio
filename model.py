import os
import logging
import json
try:
    import torch.multiprocessing as mp
    try:
        
        mp.set_start_method('spawn')
    except RuntimeError:
        pass
except ImportError:
    import multiprocessing as mp

from semver import Version
from typing import Callable, Union, List, Dict, Optional
from abc import ABC
from colorama import Fore
from label_studio_sdk.label_interface import LabelInterface
from label_studio_sdk._extensions.label_studio_tools.core.label_config import parse_config
from response import ModelResponse
from cache import create_cache

logger = logging.getLogger(__name__)

CACHE = create_cache(
    os.getenv('CACHE_TYPE', 'sqlite'),
    path=os.getenv('MODEL_DIR', '.'))


# Decorator to register predict function
_predict_fn: Callable = None
_update_fn: Callable = None


def predict_fn(f):
    global _predict_fn
    _predict_fn = f
    logger.info(f'{Fore.GREEN}Predict function "{_predict_fn.__name__}" registered{Fore.RESET}')
    return f


def update_fn(f):
    global _update_fn
    _update_fn = f
    logger.info(f'{Fore.GREEN}Update function "{_update_fn.__name__}" registered{Fore.RESET}')
    return f


class   LabelStudioMLBase(ABC):

    INITIAL_MODEL_VERSION = "0.0.1"
    TRAIN_EVENTS = (
        'ANNOTATION_CREATED',
        'ANNOTATION_UPDATED',
        'ANNOTATION_DELETED',
        'START_TRAINING'
    )

    def __init__(self, project_id: Optional[str] = None, label_config=None):

        self.project_id = project_id or ''
        if label_config is not None:
            self.use_label_config(label_config)
        else:
            logger.warning('Label config is not provided')

        # set initial model version
        if not self.model_version:
            self.set("model_version", self.INITIAL_MODEL_VERSION)
        
        self.setup()
        
    def setup(self):
        """Abstract method for setting up the machine learning model.
        This method should be overridden by subclasses of
        LabelStudioMLBase to conduct any necessary setup steps, for
        example to set model_version
        """
        
        # self.set("model_version", "0.0.2")
        

    def use_label_config(self, label_config: str):
        """
        Apply label configuration and set the model version and parsed label config.

        Args:
            label_config (str): The label configuration.
        """
        self.label_interface = LabelInterface(config=label_config)

        # if not current_label_config:
            # first time model is initialized
            # self.set('model_version', 'INITIAL')

        current_label_config = self.get('label_config')
        # label config has been changed, need to save
        if current_label_config != label_config:
            self.set('label_config', label_config)
            self.set('parsed_label_config', json.dumps(parse_config(label_config)))


    def set_extra_params(self, extra_params):
        """Set extra parameters. Extra params could be used to pass
        any additional static metadata from Label Studio side to ML
        Backend.

        Args:
            extra_params: Extra parameters to set.

        """
        self.set('extra_params', extra_params)

    @property
    def extra_params(self):
        """
        Get the extra parameters.

        Returns:
            json: If parameters exist, returns parameters in JSON format. Else, returns None.
        """
        # TODO this needs to have exception
        params = self.get('extra_params')
        if params:
            return json.loads(params)
        else:
            return {}
            
    def get(self, key: str):
        return CACHE[self.project_id, key]

    def set(self, key: str, value: str):
        CACHE[self.project_id, key] = value

    def has(self, key: str):
        return (self.project_id, key) in CACHE

    @property
    def label_config(self):
        return self.get('label_config')



    @property
    def model_version(self):
        mv = self.get('model_version')
        if mv:
            try:
                sv = Version.parse(mv)
                return sv
            except:
                return mv
        else:
            return None


    # @abstractmethod
    def predict(self, tasks: List[Dict], context: Optional[Dict] = None, **kwargs) -> Union[List[Dict], ModelResponse]:

        if _predict_fn:
            return _predict_fn(tasks, context, helper=self, **kwargs)

    def process_event(self, event, data, job_id, additional_params):
        """
        Process a given event. If event is of TRAIN type, start fitting the model.

        Args:
          event: Current event to process.
          data: The data relevant to the event.
          job_id: ID of the job related to the event.
          additional_params: Additional parameters to be processed.
        """
        if event in self.TRAIN_EVENTS:
            logger.debug(f'Job {job_id}: Received event={event}: calling {self.__class__.__name__}.fit()')
            train_output = self.fit(event=event, data=data, job_id=job_id, **additional_params)
            logger.debug(f'Job {job_id}: Train finished.')
            return train_output

    def build_label_map(self, tag_name: str, names: List[str]) -> Dict[str, str]:

        label_map = {}
        labels_attrs = self.label_interface.get_control(tag_name).labels_attrs

        model_labels = list(names)
        model_labels_lower = [label.lower() for label in model_labels]
        logger.debug(f"Labels supported by model for {tag_name}: {names}")

        if labels_attrs:
            for ls_label, label_tag in labels_attrs.items():
                # try to find `predicted_values` in Label tags
                predicted_values = label_tag.attr.get("predicted_values", "").split(",")
                matched = False
                for value in predicted_values:
                    value = value.strip()  # remove spaces at the beginning and at the end
                    if value and value in names:  # check if value is in model labels
                        if value not in model_labels:
                            logger.warning(f'Predicted value "{value}" is not in model labels')
                        label_map[value] = ls_label
                        matched = True

                # no `predicted_values`, use common Label's `value` attribute
                if not matched:
                    # model has the same label
                    if ls_label in model_labels:
                        label_map[ls_label] = ls_label
                    # model has the same lower name
                    elif ls_label.lower() in model_labels_lower:
                        label_map[ls_label.lower()] = ls_label

        logger.debug(f"Model Labels <=> Label Studio Labels:\n{label_map}")
        return label_map
    def fit(self, event, data, **additional_params):

        if _update_fn:
            return _update_fn(event, data, helper=self, **additional_params)

