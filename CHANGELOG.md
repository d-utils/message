# Changelog

All notable changes to this project will be documented in this file. See [standard-version](https://github.com/conventional-changelog/standard-version) for commit guidelines.

### [2.0.2](https://github.com/d-utils/message/compare/v2.0.1...v2.0.2) (2022-08-15)

### [2.0.1](https://github.com/d-utils/message/compare/v2.0.0...v2.0.1) (2020-06-07)


### Bug Fixes

* **Client#connect:** pass on user/pass correctly ([4c75f90](https://github.com/d-utils/message/commit/4c75f900b44f38fe3727e48d61ee4a34f448ad77))

## [2.0.0](https://github.com/d-utils/message/compare/v1.5.0...v2.0.0) (2020-06-02)


### ⚠ BREAKING CHANGES

* **expires:** Remove "Payload" from all standard payload structs. Set
default expiration to 3600 seconds from message creation. Run callback
with ResponseTimeout type whenever a request message expires.
* **message:** Removes [Message].isError property accessor
* **ack:** Will not run subscription callback when a message
cannot be parsed.

### Features

* **ack:** requeue failed messages ([92bcad6](https://github.com/d-utils/message/commit/92bcad69afe6e777bab66fa25bc243a0f87f7c6b))
* **expires:** expires messages ([2f1d36b](https://github.com/d-utils/message/commit/2f1d36b8f3a5b91477d8cf279230edda8842eea4))


* **message:** remove unused isError property ([f086b46](https://github.com/d-utils/message/commit/f086b46e007c165abd41fd96e5c0f17f3ac3303f))

## [1.5.0](https://github.com/d-utils/message/compare/v1.4.1...v1.5.0) (2020-05-31)


### Features

* **serialization:** add toJSON and toString ([3522708](https://github.com/d-utils/message/commit/35227081fdd14afdcbbbc68b1cbd85248f2ff52d))

### [1.4.1](https://github.com/d-utils/message/compare/v1.4.0...v1.4.1) (2020-05-29)

## [1.4.0](https://github.com/d-utils/message/compare/v1.3.0...v1.4.0) (2020-05-29)


### Features

* **payload:** add BAD_REQUEST and INTERNAL_ERROR ([f662edf](https://github.com/d-utils/message/commit/f662edfb137ac096ee38bd03dbfb00ed2b02cc0a))

## [1.3.0](https://github.com/d-utils/message/compare/v1.2.0...v1.3.0) (2020-05-29)


### Features

* **headers:** pass token and responseStatus ([e78ebba](https://github.com/d-utils/message/commit/e78ebba053134aed416b2fc12f83003679d95d62))

## [1.2.0](https://github.com/d-utils/message/compare/v1.1.3...v1.2.0) (2020-05-20)


### Features

* **Client#keepUpdating:** update until disconnect ([b778a20](https://github.com/d-utils/message/commit/b778a20a69bff2e33ab0c643e3584a5461fc6dbb))
* **Message#to:** convert Message to struct ([6618b05](https://github.com/d-utils/message/commit/6618b055b348d5465004fe93e0ea04407d6a3fae))

### [1.1.3](https://github.com/d-utils/message/compare/v1.1.2...v1.1.3) (2020-05-16)


### Bug Fixes

* **ci:** test setup ([e4e4b24](https://github.com/d-utils/message/commit/e4e4b24a1e5183a547f0c7d2e29e52300154ee5a))

### [1.1.2](https://github.com/d-utils/message/compare/v1.1.1...v1.1.2) (2020-05-16)


### Bug Fixes

* bad dutils-data version in dub.selections ([fe48637](https://github.com/d-utils/message/commit/fe4863766e18c952cd8b97f8d6dc29efac5c0874))

### [1.1.1](https://github.com/d-utils/message/compare/v1.1.0...v1.1.1) (2020-05-16)


### Bug Fixes

* dutils-data dependency source ([351d786](https://github.com/d-utils/message/commit/351d786407579203c9d4db0ce0430ce80cd703a4))

## 1.1.0 (2020-05-16)


### Features

* initial message features ([00b406b](https://github.com/d-utils/message/commit/00b406bdf9f0dafebe8e84aabc9db738230dd507))
