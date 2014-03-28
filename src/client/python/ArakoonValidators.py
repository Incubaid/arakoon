"""
Copyright (2010-2014) INCUBAID BVBA

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
"""




from ArakoonExceptions import ArakoonInvalidArguments

import ArakoonProtocol
import logging
from functools import wraps

class SignatureValidator :
    def __init__ (self, *args ):
        self.param_types = args
        self.param_native_type_mapping = {
            'int': int,
            'string': str,
            'bool': bool
        }

    def __call__ (self, f ):
        @wraps(f)
        def my_new_f ( *args, **kwargs ) :
            new_args = list( args[1:] )
            missing_args = f.func_code.co_varnames[len(args):]
            for missing_arg in missing_args:
                if( len(new_args) == len(self.param_types) ) :
            
                    break
                if( kwargs.has_key(missing_arg) ) :
                    pos = f.func_code.co_varnames.index( missing_arg )
#                    if pos > len(new_args):
#                        new_args.append( None )
            
                    new_args.insert(pos, kwargs[missing_arg])
                    del kwargs[missing_arg]
            
            if len( kwargs ) > 0:
                raise ArakoonInvalidArguments( f.func_name, list(kwargs.iteritems()) )
            
            i = 0
            error_key_values = []
            for (arg, arg_type) in zip(new_args, self.param_types) :
                if not self.validate(arg, arg_type):
                    error_key_values.append( (f.func_code.co_varnames[i+1],new_args[i]) )
                i += 1
                
            if len(error_key_values) > 0 :
                raise ArakoonInvalidArguments( f.func_name, error_key_values )
            
            return f( args[0], *new_args )
        
        return my_new_f

    def validate(self,arg,arg_type):
        if self.param_native_type_mapping.has_key( arg_type ):
            return isinstance(arg,self.param_native_type_mapping[arg_type] )     
        elif arg_type == 'string_option' :
            return isinstance( arg, str ) or arg is None
        elif arg_type == 'sequence' :
            return isinstance( arg, ArakoonProtocol.Sequence )
        else:
            raise RuntimeError( "Invalid argument type supplied: %s" % arg_type )
        
    