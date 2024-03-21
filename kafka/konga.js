Welcome to Ubuntu 22.04.2 LTS (GNU/Linux 5.15.0-73-generic x86_64)

 * Documentation:  https://help.ubuntu.com
 * Management:     https://landscape.canonical.com
 * Support:        https://ubuntu.com/advantage

  System information as of Sat Jul 15 04:04:44 PM UTC 2023

  System load:                      0.0
  Usage of /:                       16.8% of 95.90GB
  Memory usage:                     10%
  Swap usage:                       0%
  Processes:                        266
  Users logged in:                  1
  IPv4 address for br-a25185d372f9: 172.18.0.1
  IPv4 address for docker0:         172.17.0.1
  IPv4 address for ens18:           172.16.23.243
  IPv4 address for ens19:           121.58.233.74

 * Strictly confined Kubernetes makes edge and IoT secure. Learn how MicroK8s
   just raised the bar for easy, resilient and secure K8s cluster deployment.

   https://ubuntu.com/engage/secure-kubernetes-at-the-edge

Expanded Security Maintenance for Applications is not enabled.

20 updates can be applied immediately.
To see these additional updates run: apt list --upgradable

Enable ESM Apps to receive additional future security updates.
See https://ubuntu.com/esm or run: sudo pro status


*** System restart required ***
Last login: Sat Jul 15 02:03:00 2023 from 136.158.78.16
gie@pw-pv-apim:~$ cd konga/
gie@pw-pv-apim:~/konga$ ls
api     assets  bower.json  CHANGELOG.md  config              Dockerfile  dummy.js             Gruntfile.js  makedb        package.json       README.md   screenshots  tasks  views
app.js  bin     certs       charts        docker-compose.yml  docs        ecosystem.config.js  LICENSE       node_modules  package-lock.json  ROADMAP.md  start.sh     test
gie@pw-pv-apim:~/konga$ vi .env
gie@pw-pv-apim:~/konga$ sudo npm start
[sudo] password for gie: 

> kongadmin@0.14.9 start
> node --harmony app.js

debug: Warning: Grunt functionality may not work properly with your current configuration.
debug: Run `npm install sails-hook-grunt --save` to continue using Grunt with your app.
debug: 
error: Failed to lift app: Error: Invalid `hookTimeout` config!  If set, this should be a positive whole number, but instead got `330000`.  Please change this setting, then try lifting again.
    at loadHook (/home/gie/konga/node_modules/sails/lib/app/private/loadHooks.js:172:21)
    at userhooks (/home/gie/konga/node_modules/sails/lib/app/private/loadHooks.js:291:11)
    at /home/gie/konga/node_modules/sails/node_modules/async/dist/async.js:3883:24
    at replenish (/home/gie/konga/node_modules/sails/node_modules/async/dist/async.js:1014:17)
    at iterateeCallback (/home/gie/konga/node_modules/sails/node_modules/async/dist/async.js:998:17)
    at /home/gie/konga/node_modules/sails/node_modules/async/dist/async.js:972:16
    at /home/gie/konga/node_modules/sails/node_modules/async/dist/async.js:3888:13
    at /home/gie/konga/node_modules/sails/lib/app/private/loadHooks.js:241:38
    at process.processTicksAndRejections (node:internal/process/task_queues:77:11)
gie@pw-pv-apim:~/konga$ less /home/gie/konga/node_modules/sails/lib/app/private/loadHooks.js
gie@pw-pv-apim:~/konga$ less /home/gie/konga/node_modules/sails/lib/app/private/loadHooks.js -n
gie@pw-pv-apim:~/konga$ less -n /home/gie/konga/node_modules/sails/lib/app/private/loadHooks.js 
gie@pw-pv-apim:~/konga$ less --LINE-NUMBERS /home/gie/konga/node_modules/sails/lib/app/private/loadHooks.js
gie@pw-pv-apim:~/konga$ less --LINE-NUMBERS /home/gie/konga/node_modules/sails/lib/app/private/loadHooks.js
gie@pw-pv-apim:~/konga$ cat /home/gie/konga/node_modules/sails/lib/app/private/loadHooks.js
/**
 * Module dependencies
 */

var util = require('util');
var _ = require('@sailshq/lodash');
var async = require('async');
var defaultsDeep = require('merge-defaults');// « TODO: Get rid of this
var __hooks = require('../../hooks');




/**
 * @param  {SailsApp} sails
 * @returns {Function}
 */
module.exports = function(sails) {

  var Hook = __hooks(sails);

  // Keep an array of all the hook timeouts.
  // This way if a hook fails to load, we can clear all the timeouts at once.
  var hookTimeouts = [];
  // NOTE: There's no particular reason this (^^) is outside of the function being returned below.
  // - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
  // FUTURE: pull it in below to avoid leading to any incorrect assumptions about race conditions, etc.)
  // - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

  /**
   * Resolve the hook definitions and then finish loading them
   *
   * @api private
   */
  return function initializeHooks(hooks, cb) {


    // ==============================================================================
    // < inline function declarations >
    //    ██╗    ██╗███╗   ██╗██╗     ██╗███╗   ██╗███████╗    ███████╗███╗   ██╗    ██████╗ ███████╗███████╗███████╗    ██╗
    //   ██╔╝    ██║████╗  ██║██║     ██║████╗  ██║██╔════╝    ██╔════╝████╗  ██║    ██╔══██╗██╔════╝██╔════╝██╔════╝    ╚██╗
    //  ██╔╝     ██║██╔██╗ ██║██║     ██║██╔██╗ ██║█████╗      █████╗  ██╔██╗ ██║    ██║  ██║█████╗  █████╗  ███████╗     ╚██╗
    //  ╚██╗     ██║██║╚██╗██║██║     ██║██║╚██╗██║██╔══╝      ██╔══╝  ██║╚██╗██║    ██║  ██║██╔══╝  ██╔══╝  ╚════██║     ██╔╝
    //   ╚██╗    ██║██║ ╚████║███████╗██║██║ ╚████║███████╗    ██║     ██║ ╚████║    ██████╔╝███████╗██║     ███████║    ██╔╝
    //    ╚═╝    ╚═╝╚═╝  ╚═══╝╚══════╝╚═╝╚═╝  ╚═══╝╚══════╝    ╚═╝     ╚═╝  ╚═══╝    ╚═════╝ ╚══════╝╚═╝     ╚══════╝    ╚═╝
    //

    // - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    // FUTURE: extrapolate the following three inline function definitions
    // into separate files.
    // - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    /**
     * FUTURE: extrapolate
     * @param  {[type]} id [description]
     * @return {[type]}    [description]
     */
    function prepareHook(id) {

      var rawHookFn = hooks[id];

      // Backwards compatibility:
      if (rawHookFn === 'false') {

        // FUTURE: Do not allow the string "false" here (now that all environment variables
        // are handled via rttc.parseHuman, this is no longer necessary)
        sails.log.debug('The string "false" was configured for `sails.config.hooks[\''+id+'\']`.');
        sails.log.debug('For compatibility\'s sake, automatically changing this to `false` (boolean).');
        sails.log.debug('(Note that this backwards-compatibility check will be removed in a future');
        sails.log.debug('release of Sails, so be sure to update this app ASAP.)');
        rawHookFn = false;

      }//>-


      // - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
      // COMPATIBILITY NOTE:
      // There used to be a check here, to the effect of this:
      // ```
      // // Check if this hook has a dot in the name.
      // // If so, something is wrong.
      // var doesHookHaveDotInName = !!id.match(/\./);
      // if (doesHookHaveDotInName) {
      // var partBeforeDot = id.split('.')[0];
      // hooks[partBeforeDot] = false;
      // ```
      //
      // But it was removed in Sails v1, since it was no longer relevant.
      // - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -


      // Allow disabling of hooks by setting them to `false`.
      if (rawHookFn === false) {
        delete hooks[id];
        return;
      }

      // Check for invalid hook config
      if (hooks.userconfig && !hooks.moduleloader) {
        return cb('Invalid configuration:: Cannot use the `userconfig` hook w/o the `moduleloader` hook enabled!');
      }

      // Handle folder-defined modules (default to index.js)
      // Since a hook definition must be a function
      if (_.isObject(rawHookFn) && !_.isArray(rawHookFn) && !_.isFunction(rawHookFn)) {
        rawHookFn = rawHookFn.index;
      }

      if (!_.isFunction(rawHookFn)) {
        sails.log.error('Malformed hook! (' + id + ')');
        sails.log.error('Hooks should be a function with one argument (`sails`)');
        sails.log.error('But instead, got:', rawHookFn);
        process.exit(1);
      }

      // Instantiate the hook
      var def = rawHookFn(sails);

      // Mix in an `identity` property to hook definition
      def.identity = id.toLowerCase();

      // If a config key was defined for this hook when it was loaded,
      // (probably because a user is overridding the default config key)
      // set it on the hook definition
      def.configKey = rawHookFn.configKey || def.identity;

      // New up an actual Hook instance
      hooks[id] = new Hook(def);
    }//ƒ


    /**
     * Apply a hook's "defaults" property
     *
     * FUTURE: extrapolate
     *
     * @param  {[type]} hook [description]
     * @return {[type]}      [description]
     */
    function applyDefaults(hook) {

      // Get the hook defaults
      var defaults = (_.isFunction(hook.defaults) ?
                            hook.defaults(sails.config) :
                            hook.defaults) || {};

      // Replace the special __configKey__ key with the actual config key
      if (hook.defaults.__configKey__ && hook.configKey) {
        hook.defaults[hook.configKey] = hook.defaults.__configKey__;
        delete hook.defaults.__configKey__;
      }

      defaultsDeep(sails.config, defaults);
    }//ƒ

    /**
     * Load a hook (bind its routes, load any modules and initialize it)
     *
     * FUTURE: extrapolate
     *
     * @param  {[type]}   id [description]
     * @param  {Function} cb [description]
     * @return {[type]}      [description]
     */
    function loadHook(id, cb) {
      // TODO: refactor this^^^
      //  (no need for an inline function declaration)

      // Validate `hookTimeout` setting, if present.
      if (!_.isUndefined(sails.config.hookTimeout)) {
        sails.config.hookTimeout = Number(sails.config.hookTimeout)
        if (!_.isNumber(sails.config.hookTimeout) || sails.config.hookTimeout < 1 || Math.floor(sails.config.hookTimeout) !== sails.config.hookTimeout) {
          return cb(new Error('Invalid `hookTimeout` config!  If set, this should be a positive whole number, but instead got `'+sails.config.hookTimeout+'`.  Please change this setting, then try lifting again.'));
        }
      }

      var timestampBeforeLoad = Date.now();
      var DEFAULT_HOOK_TIMEOUT = 40000;
      var timeoutInterval = (sails.config[hooks[id].configKey || id] && sails.config[hooks[id].configKey || id]._hookTimeout) || sails.config.hookTimeout || DEFAULT_HOOK_TIMEOUT;

      var hookTimeout;
      if (id !== 'userhooks') {
        hookTimeout = setTimeout(function tooLong() {
          // - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
          // FUTURE: sniff hook id here to improve error msg, e.g.:
          // ```
          // ((id === 'grunt') ? 'It looks like Grunt is still compiling your assets.' : '...')
          // ```
          // ^^But note that this would require a bit more work: currently, the id here isn't
          // necessarily the hook that timed out.  (It could be a dependent hook.)
          // - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
          var err = new Error(
            'Sails is taking too long to load.\n'+
            '\n'+
            '--  --  --  --  --  --  --  --  --  --  --  --  --  --  --  --  --  --  --  --  --\n'+
            ' Troubleshooting tips:\n'+
            '  -• Were you still reading/responding to an interactive prompt?\n'+
            '     (Whoops, sorry!  Please lift again and try to respond a bit more quickly.)\n'+
            '\n'+
            '  -• Do you have a lot of stuff in `assets/`?  Grunt might still be running.\n'+
            '    (Try increasing the hook timeout.  Currently it is '+(sails.config.hookTimeout||DEFAULT_HOOK_TIMEOUT)+'.\n'+
            '     e.g. `sails lift --hookTimeout='+(Math.max(DEFAULT_HOOK_TIMEOUT, 2*(sails.config.hookTimeout||DEFAULT_HOOK_TIMEOUT)))+'`)\n'+
            '\n'+
            '  -• Is `'+id+'` a custom or 3rd party hook?\n'+
            '    (*If* `initialize()` is using a callback, make sure it\'s being called.)\n'+
            '--  --  --  --  --  --  --  --  --  --  --  --  --  --  --  --  --  --  --  --  --\n'
          );
          err.code = 'E_HOOK_TIMEOUT';
          cb(err);
        }, timeoutInterval);
        hookTimeouts.push(hookTimeout);
      }
      hooks[id].load(function(err) {

        // Sanity check: (see https://trello.com/c/1jCljHHP for an example of a
        // potential bug this catches)
        if (!process.nextTick) {
          return cb(new Error('Consistency violation: Hmm... it looks like something is wrong with Node\'s `process` global.  Check it out:\n'+util.inspect(process)));
        }

        if (id !== 'userhooks') {
          clearTimeout(hookTimeout);
        }
        if (err) {
          // Clear all hook timeouts so that the process doesn't hang because
          // something is waiting for this failed hook to load.
          _.each(hookTimeouts, function(hookTimeout) {clearTimeout(hookTimeout);});
          if (id !== 'userhooks') {
            sails.log.error('A hook (`' + id + '`) failed to load!');
          }
          sails.emit('hook:' + id + ':error');

          // Defer a tick to allow other stuff to happen
          process.nextTick(function(){ cb(err); });
          return;
        }

        sails.log.verbose(id, 'hook loaded successfully. ('+(Date.now() - timestampBeforeLoad)+'ms)');
        sails.emit('hook:' + id + ':loaded');

        // Defer a tick to allow other stuff to happen
        process.nextTick(function(){ cb(); });
      });
    }//ƒ

    //    ██╗    ██╗    ██╗███╗   ██╗██╗     ██╗███╗   ██╗███████╗    ███████╗███╗   ██╗    ██████╗ ███████╗███████╗███████╗    ██╗
    //   ██╔╝   ██╔╝    ██║████╗  ██║██║     ██║████╗  ██║██╔════╝    ██╔════╝████╗  ██║    ██╔══██╗██╔════╝██╔════╝██╔════╝    ╚██╗
    //  ██╔╝   ██╔╝     ██║██╔██╗ ██║██║     ██║██╔██╗ ██║█████╗      █████╗  ██╔██╗ ██║    ██║  ██║█████╗  █████╗  ███████╗     ╚██╗
    //  ╚██╗  ██╔╝      ██║██║╚██╗██║██║     ██║██║╚██╗██║██╔══╝      ██╔══╝  ██║╚██╗██║    ██║  ██║██╔══╝  ██╔══╝  ╚════██║     ██╔╝
    //   ╚██╗██╔╝       ██║██║ ╚████║███████╗██║██║ ╚████║███████╗    ██║     ██║ ╚████║    ██████╔╝███████╗██║     ███████║    ██╔╝
    //    ╚═╝╚═╝        ╚═╝╚═╝  ╚═══╝╚══════╝╚═╝╚═╝  ╚═══╝╚══════╝    ╚═╝     ╚═╝  ╚═══╝    ╚═════╝ ╚══════╝╚═╝     ╚══════╝    ╚═╝
    //
    // </ inline function declarations (see note above) >
    // ==============================================================================


    // Now do a few things, one after another.
    async.series(
      {

        // First load the moduleloader (if any)
        moduleloader: function(cb) {
          if (!hooks.moduleloader) {
            return cb();
          }
          prepareHook('moduleloader');
          applyDefaults(hooks['moduleloader']);
          hooks['moduleloader'].configure();
          loadHook('moduleloader', cb);
        },

        // Next load the user config (if any)
        userconfig: function(cb) {
          if (!hooks.userconfig) {
            return cb();
          }
          prepareHook('userconfig');
          applyDefaults(hooks['userconfig']);
          hooks['userconfig'].configure();
          loadHook('userconfig', cb);
        },

        // Next get the user hooks (if any), which will be
        // added to the list of hooks to load
        userhooks: function(cb) {
          if (!hooks.userhooks) {
            return cb();
          }
          prepareHook('userhooks');
          applyDefaults(hooks['userhooks']);
          hooks['userhooks'].configure();
          loadHook('userhooks', cb);
        },

        validate: function(cb) {
          if (hooks.controllers) {
            sails.log.debug('=================================================================================');
            sails.log.debug('Ignoring `controllers` hook:');
            sails.log.debug('As of Sails v1, `controllers` can no longer be disabled/enabled as hooks.');
            sails.log.debug('Instead, Sails core now understands controller actions as first-class citizens.');
            sails.log.debug('See the Sails v1.0 upgrade guide: http://sailsjs.com/upgrading');
            sails.log.debug('=================================================================================');
            delete hooks.controllers;
          }
          return cb();
        },

        // Prepare all other hooks
        prepare: function(cb) {
          async.each(_.without(_.keys(hooks), 'userconfig', 'moduleloader', 'userhooks'), function (id, cb) {
            prepareHook(id);
            // Defer to next tick to allow other stuff to happen
            process.nextTick(cb);
          }, cb);
        },

        // Apply the default config for all other hooks
        defaults: function(cb) {
          async.each(_.without(_.keys(hooks), 'userconfig', 'moduleloader', 'userhooks'), function (id, cb) {
            var hook = hooks[id];
            applyDefaults(hook);
            // Defer to next tick to allow other stuff to happen
            process.nextTick(cb);
          }, cb);
        },

        // Run configuration method for all other hooks
        configure: function(cb) {
          async.each(_.without(_.keys(hooks), 'userconfig', 'moduleloader', 'userhooks'), function (id, cb) {
            var hook = hooks[id];
            try {
              hook.configure();
            } catch (err) {
              return process.nextTick(function(){ cb(err); });
            }
            // Defer to next tick to allow other stuff to happen
            process.nextTick(cb);
          }, cb);
        },

        // Load all other hooks
        load: function(cb) {
          async.each(_.without(_.keys(hooks), 'userconfig', 'moduleloader', 'userhooks'), function (id, cb) {
            sails.log.silly('Loading hook: ' + id);
            loadHook(id, cb);
          }, cb);
        }
      },

      function afterwards(err) {
        if (err) { return cb(err); }
        return cb();
      }
    );//</async.series>
  };
};
