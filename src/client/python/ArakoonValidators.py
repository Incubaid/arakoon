'''
This file is part of Arakoon, a distributed key-value store. Copyright
(C) 2010 Incubaid BVBA

Licensees holding a valid Incubaid license may use this file in
accordance with Incubaid's Arakoon commercial license agreement. For
more information on how to enter into this agreement, please contact
Incubaid (contact details can be found on www.arakoon.org/licensing).

Alternatively, this file may be redistributed and/or modified under
the terms of the GNU Affero General Public License version 3, as
published by the Free Software Foundation. Under this license, this
file is distributed in the hope that it will be useful, but WITHOUT
ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
FITNESS FOR A PARTICULAR PURPOSE.

See the GNU Affero General Public License for more details.
You should have received a copy of the
GNU Affero General Public License along with this program (file "COPYING").
If not, see <http://www.gnu.org/licenses/>.
'''


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
        
    