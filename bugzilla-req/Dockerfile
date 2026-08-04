# Requirement-intake portal web image (REQ-4 + REQ-5a of docs/req-portal-bugzilla-keycloak-sso.md).
#
# TRACEABLE + IMMUTABLE-AFTER-BUILD, deliberately NOT called "reproducible": an apt-based build is
# not input-locked (archive metadata and package versions move under us). What IS guaranteed is that
# every input is named — pinned base digest, pinned upstream commit — and the built image is
# consumed by digest.
#
# The upstream Bugzilla tree is NOT vendored here. The Jenkins job clones
# github.com/bugzilla/bugzilla at the pinned commit into this build context first
# (scripts/deploy/bugzilla-req.sh), so `COPY . /var/www/html` below is that exact tree — the
# same commit the internal instance runs, which is what makes the REQ-5c source-pinned auth
# behaviour apply to this image too.
#
# This mirrors the upstream 5.2 Dockerfile (apt set, CPAN modules, mod_perl wiring) and then adds
# the portal overlay: mod_auth_openidc plus the two-listener vhost split. Keeping the upstream steps
# inline rather than deriving FROM the .252-local image is deliberate — the portal must not depend
# on an image that was built by hand on the production host and is not in Git.
FROM ubuntu:22.04@sha256:3b06811b2afd352be909dd088a004166d665dc76d38b13eada33522a9d915c6f

ENV DEBIAN_FRONTEND=noninteractive

# --- upstream Bugzilla 5.2 runtime dependencies (mirrors the upstream Dockerfile) ---------------
RUN apt-get update && apt-get -y dist-upgrade
RUN apt-get -y install \
 apache2 \
 mariadb-client \
 netcat-traditional \
 libappconfig-perl \
 libdate-calc-perl \
 libtemplate-perl \
 build-essential \
 libdatetime-timezone-perl \
 libdatetime-perl \
 libemail-address-perl \
 libemail-sender-perl \
 libemail-mime-perl \
 libemail-mime-modifier-perl \
 libdbi-perl \
 libdbix-connector-perl \
 libcgi-pm-perl \
 liblocale-codes-perl \
 libmath-random-isaac-perl \
 libmath-random-isaac-xs-perl \
 libapache2-mod-perl2 \
 libapache2-mod-perl2-dev \
 libchart-perl \
 libxml-perl \
 libxml-twig-perl \
 perlmagick \
 libgd-graph-perl \
 libtemplate-plugin-gd-perl \
 libsoap-lite-perl \
 libhtml-scrubber-perl \
 libjson-rpc-perl \
 libdaemon-generic-perl \
 libtheschwartz-perl \
 libtest-taint-perl \
 libauthen-radius-perl \
 libfile-slurp-perl \
 libencode-detect-perl \
 libmodule-build-perl \
 libnet-ldap-perl \
 libauthen-sasl-perl \
 libfile-mimeinfo-perl \
 libhtml-formattext-withlinks-perl \
 libgd-dev \
 libmysqlclient-dev \
 graphviz \
 vim-common \
 python3

# Ubuntu 22.04 does not ship new enough versions of these, so they come from CPAN (upstream does the
# same). This is an unpinned network fetch — the honest limit on this image's traceability, and the
# reason REQ-4 claims immutability-after-build rather than reproducibility.
RUN cpan install Template::Toolkit Email::Address::XS Email::Sender DBD::MariaDB

# --- portal overlay: OIDC module (REQ-5a) -------------------------------------------------------
RUN apt-get -y install libapache2-mod-auth-openidc \
 && a2enmod auth_openidc \
 && a2enmod headers

WORKDIR /var/www/html
COPY --chown=root:www-data . /var/www/html
COPY ./docker /root/docker

# Portal vhosts replace the upstream single default vhost. Two REAL Apache listeners, one vhost set
# each (REQ-5a): the public listener carries no native-login vhost at all, so no Host header,
# absolute-form target, duplicate Host, HTTP/1.0 no-Host or HTTP/2 authority trick can reach native
# Bugzilla through it. ServerName is documentation here, never the access control.
COPY ./oidc/ports.conf /etc/apache2/ports.conf
COPY ./oidc/000-portal-public.conf /etc/apache2/sites-available/000-portal-public.conf
COPY ./oidc/001-portal-admin.conf /etc/apache2/sites-available/001-portal-admin.conf
COPY ./oidc/entrypoint.sh          /root/docker/entrypoint.sh
COPY ./oidc/render-answers.py     /root/docker/render-answers.py
COPY ./oidc/checksetup-answers.tmpl /root/docker/checksetup-answers.tmpl

RUN a2dissite 000-default \
 && a2ensite 000-portal-public \
 && a2ensite 001-portal-admin \
 && chmod 0755 /root/docker/entrypoint.sh /root/docker/render-answers.py

# Upstream housekeeping: keep Docker droppings out of the web root, and make the data dir writable
# by the app (it is a bind mount at runtime).
# The build context is a git CLONE of upstream, so `COPY . /var/www/html` also brings `.git` (full
# repository objects) and the build-context markers into the DOCUMENT ROOT. Even where Apache would
# deny them, an image should not carry them at all — this is the one cleanup line that is a security
# fix rather than housekeeping.
RUN rm -rf /var/www/html/.git /var/www/html/.upstream-commit /var/www/html/.dockerignore \
 && rm -rf /var/www/html/docker* /var/www/html/Dockerfile* /var/www/html/oidc \
 && rm -f  /var/www/html/RUNBOOK-app-state.md \
 && rm -rf /var/www/html/data /var/www/html/localconfig /var/www/html/index.html \
 && mkdir /var/www/html/data
RUN a2enmod expires && a2enmod rewrite && a2dismod mpm_event && a2enmod mpm_prefork

# 80 = public (OIDC-protected), 81 = admin (native login). Exposing or denying either is the
# deployment mechanism's job — see fullfunding-namespace-gate1.md §4.
EXPOSE 80/tcp 81/tcp
CMD ["/root/docker/entrypoint.sh"]
