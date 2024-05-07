#!/usr/bin/python3
# PYTHON_ARGCOMPLETE_OK

import argparse, sys, argcomplete, traceback
sys.path.insert(0, 'src')
import src.f4i_create_vsilo
import src.f4i_destroy_vsilo
import src.f4i_add_flavour
import src.f4i_delete_flavour
import src.f4i_add_thingvisor
import src.f4i_delete_thingvisor
import src.f4i_add_vthing
import src.f4i_delete_vthing
import src.f4i_inspect_tenant
import src.f4i_inspect_thingvisor
import src.f4i_inspect_flavour
import src.f4i_inspect_vsilo
import src.f4i_list_flavours
import src.f4i_list_users
import src.f4i_list_thingvisors
import src.f4i_list_vsilos
import src.f4i_list_vthings
import src.f4i_login
import src.f4i_logout
import src.f4i_register
import src.f4i_unregister
import src.f4i_update_thingvisor
import src.f4i_set_vthing_endpoint
import src.f4i_del_vthing_endpoint

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(metavar="<command>")

    # login
    parser_cs = subparsers.add_parser('login', help='log into VirIoT system')
    src.f4i_login.init_args(parser_cs)

    # logout
    parser_cs = subparsers.add_parser('logout', help='log out from VirIoT system')
    src.f4i_logout.init_args(parser_cs)

    # register
    parser_cs = subparsers.add_parser('register', help='sign into VirIoT system')
    src.f4i_register.init_args(parser_cs)

    # unregister
    parser_cs = subparsers.add_parser('unregister', help='unsign into VirIoT system')
    src.f4i_unregister.init_args(parser_cs)

    # create-vsilo
    parser_cs = subparsers.add_parser('create-vsilo', help='create an IoT vsilo')
    src.f4i_create_vsilo.init_args(parser_cs)

    # destroy-vsilo
    parser_ds = subparsers.add_parser('destroy-vsilo', help='destroy an IoT vsilo')
    src.f4i_destroy_vsilo.init_args(parser_ds)

    # add-flavour
    parser_af = subparsers.add_parser('add-flavour', help='add a IoT slice flavour')
    src.f4i_add_flavour.init_args(parser_af)

    # delete-flavour
    parser_df = subparsers.add_parser('del-flavour', help='delete a IoT slice flavour')
    src.f4i_delete_flavour.init_args(parser_df)

    # add-thingvisor
    parser_ah = subparsers.add_parser('add-thingvisor', help='add thing thingvisor')
    src.f4i_add_thingvisor.init_args(parser_ah)

    # delete-thingvisor
    parser_dh = subparsers.add_parser('del-thingvisor', help='delete thing thingvisor')
    src.f4i_delete_thingvisor.init_args(parser_dh)

    # set-vthing-endpoint
    parser_at = subparsers.add_parser('set-vthing-endpoint', help='set virtual thing http endpoint for streaming services')
    src.f4i_set_vthing_endpoint.init_args(parser_at)

    # del-vthing-endpoint
    parser_at = subparsers.add_parser('del-vthing-endpoint', help='del virtual thing http endpoint for streaming services')
    src.f4i_del_vthing_endpoint.init_args(parser_at)

    # add-vthing
    parser_at = subparsers.add_parser('add-vthing', help='add virtual thing to a tenant')
    src.f4i_add_vthing.init_args(parser_at)

    # delete-vthing
    parser_dt = subparsers.add_parser('del-vthing', help='delete a virtual thing of a tenant')
    src.f4i_delete_vthing.init_args(parser_dt)

    # inspect tenant
    parser_it = subparsers.add_parser('inspect-tenant', help='dump tenant properties')
    src.f4i_inspect_tenant.init_args(parser_it)

    # inspect thingvisor
    parser_it = subparsers.add_parser('inspect-thingvisor', help='dump thingvisor properties')
    src.f4i_inspect_thingvisor.init_args(parser_it)

    # update thingvisor
    parser_it = subparsers.add_parser('update-thingvisor', help='update thingvisor properties')
    src.f4i_update_thingvisor.init_args(parser_it)

    # inspect flavour
    parser_it = subparsers.add_parser('inspect-flavour', help='dump flavour properties')
    src.f4i_inspect_flavour.init_args(parser_it)

    # inspect vsilo
    parser_it = subparsers.add_parser('inspect-vsilo', help='dump vsilo properties')
    src.f4i_inspect_vsilo.init_args(parser_it)

    # list flavours
    parser_lf = subparsers.add_parser('list-flavours', help='dump installed IoT slice flavours')
    src.f4i_list_flavours.init_args(parser_lf)

    # list users
    parser_lf = subparsers.add_parser('list-users', help='dump registered users')
    src.f4i_list_users.init_args(parser_lf)

    # list thingvisors
    parser_lh = subparsers.add_parser('list-thingvisors', help='dump installed thing hypervisors')
    src.f4i_list_thingvisors.init_args(parser_lh)

    # list vsilos
    parser_ls = subparsers.add_parser('list-vsilos', help='dump running vsilos')
    src.f4i_list_vsilos.init_args(parser_ls)

    # list vThings
    parser_lv = subparsers.add_parser('list-vthings', help='dump available virtual things')
    src.f4i_list_vthings.init_args(parser_lv)

    argcomplete.autocomplete(parser)
    args = parser.parse_args()

    try:
        args.func(args)
    except ImportError:
        print("Import error, there are missing dependencies to install.  'apt-get install python3-argcomplete "
              "&& activate-global-python-argcomplete3' may solve")
    except AttributeError:
         parser.print_help()
    except Exception:
        traceback.print_exc()
