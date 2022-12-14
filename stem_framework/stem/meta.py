"""metadata it is user-defined tree of values.

According principle of metadata processor, during the
processing of data, only data (immutable initial data in any textual or binary format) and metadata) are allowed to be used as input, meaning no user instructions (scripts) or
manually managed intermediate states are possible."""

from dataclasses import dataclass, is_dataclass
from typing import Optional, Any, Tuple, Type, Union
from stem.core import Dataclass 

Meta = Union[dict,Dataclass]

SpecificationField = Tuple[
    object, Union[Type, Tuple[Type, ...], Type[Meta]]]

Specification = Type[Dataclass] | Tuple[SpecificationField, ...]


class SpecificationError(Exception):
    pass


@dataclass
class MetaFieldError:
    required_key: str
    required_types: Optional[tuple[type]] = None
    presented_type: Optional[type] = None
    presented_value: Any = None


class MetaVerification:

    def __init__(self, *errors: Union[MetaFieldError, "MetaVerification"]):
        self.error = errors
     
    @property
    def checked_success(self):
        flag = True
        for i in self.error:
            if isinstance(i, MetaFieldError):
                flag = False
                break
        return flag

    @staticmethod
    def verify(meta: Meta,
               specification: Optional[Specification] = None) -> "MetaVerification":

        #get keys from meta
        if is_dataclass(meta):
            meta_keys = meta.__dataclass_fields__.keys()
        elif isinstance(meta, dict): 
            meta_keys = meta.keys()
        else:
            meta_keys = ()
        # get keys from specification
        if is_dataclass(specification):
            specification_keys = specification.__dataclass_fields__.keys()
        else:
            specification = dict(specification)
            specification_keys = specification.keys()

        errors = []
        for required_key in specification_keys:
            if is_dataclass(specification):
                required_types = specification.__dataclass_fields__[required_key].type
            else:
                required_types = specification[required_key]

            if required_key not in meta_keys:
                errors.append(
                    MetaFieldError(
                        required_key = required_key,
                        required_types = required_types
                    )
                )
            
        return MetaVerification(*errors)



def get_meta_attr(meta : Meta, key : str, default : Optional[Any] = None) -> Optional[Any]:
    if type(meta)==dict:
        try:
            return (meta[key])
        except KeyError():
            return (default)
    else:
        try:
            return(getattr(meta, key))
        except AttributeError:
            return(default)
def update_meta(meta: Meta, **kwargs):
    if type(meta)==dict:
        for k,a in kwargs.items():
            meta[k]=a
    else:
        for k,a in kwargs.items():
            setattr(meta, k, a)

