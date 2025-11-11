import asyncio
from functools import update_wrapper
import inspect

import grpc
from django.db.models.query import QuerySet

from django_grpc_framework.signals import grpc_request_started, grpc_request_finished


class Service:
    def __init__(self, **kwargs):
        for key, value in kwargs.items():
            setattr(self, key, value)

    @classmethod
    def as_servicer(cls, **initkwargs):
        """
        Returns a gRPC servicer instance::

            servicer = PostService.as_servicer()
            add_PostControllerServicer_to_server(servicer, server)
        """
        for key in initkwargs:
            if not hasattr(cls, key):
                raise TypeError(
                    "%s() received an invalid keyword %r. as_servicer only "
                    "accepts arguments that are already attributes of the "
                    "class." % (cls.__name__, key)
                )
        if isinstance(getattr(cls, "queryset", None), QuerySet):

            def force_evaluation():
                raise RuntimeError(
                    "Do not evaluate the `.queryset` attribute directly, "
                    "as the result will be cached and reused between requests."
                    " Use `.all()` or call `.get_queryset()` instead."
                )

            cls.queryset._fetch_all = force_evaluation

        class Servicer:
            def __getattr__(self, action):
                if not hasattr(cls, action):
                    return not_implemented

                controller_fn = getattr(cls, action)

                async def handler_async(request, context):
                    grpc_request_started.send(sender=handler_async, request=request, context=context)
                    try:
                        self = cls(**initkwargs)
                        self.request = request
                        self.context = context
                        self.action = action
                        controller = getattr(self, action)
                        result = controller(request, context)
                        if inspect.isawaitable(result):
                            return await result
                        return result
                    finally:
                        grpc_request_finished.send(sender=handler_async)

                def handler_sync(request, context):
                    grpc_request_started.send(sender=handler_sync, request=request, context=context)
                    try:
                        self = cls(**initkwargs)
                        self.request = request
                        self.context = context
                        self.action = action
                        controller = getattr(self, action)
                        result = controller(request, context)
                        if inspect.isawaitable(result):
                            # No running loop in gRPC sync worker threads; run the coroutine to completion here.
                            return asyncio.run(result)
                        return result
                    finally:
                        grpc_request_finished.send(sender=handler_sync)

                # Choose the appropriate handler based on whether the view method is async.
                if inspect.iscoroutinefunction(controller_fn):
                    update_wrapper(handler_async, controller_fn)
                    return handler_async
                else:
                    update_wrapper(handler_sync, controller_fn)
                    return handler_sync

        update_wrapper(Servicer, cls, updated=())
        return Servicer()


def not_implemented(request, context):
    """Method not implemented"""
    context.set_code(grpc.StatusCode.UNIMPLEMENTED)
    context.set_details("Method not implemented!")
    raise NotImplementedError("Method not implemented!")
