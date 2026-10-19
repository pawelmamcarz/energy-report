FROM caddy:2.10.2-alpine
COPY index.html energy_analysis_v4.py energy_analysis_v5.py /srv/
COPY Caddyfile /etc/caddy/Caddyfile
EXPOSE 8080
CMD ["caddy", "run", "--config", "/etc/caddy/Caddyfile", "--adapter", "caddyfile"]
