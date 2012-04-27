open Bstore
open Mp_driver

module BADispatcher = Dispatcher.ADispatcher(BStore)
module FSMDriver = MPDriver(BADispatcher)

module DISPATCHER = (BADispatcher : Mp.MP_ACTION_DISPATCHER)
module DRIVER = FSMDriver

