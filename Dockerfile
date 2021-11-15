FROM tercen/pg_prediction:1.0.1

ENV RENV_VERSION 0.13.2

RUN R -e "install.packages('remotes', repos = c(CRAN = 'https://cran.r-project.org'))"
RUN R -e "remotes::install_github('rstudio/renv@${RENV_VERSION}')"

COPY . /operator

WORKDIR /operator

RUN R -e "options(HTTPUserAgent='R/4.0.4 R (4.0.4 debian:bullseye-slim x86_64 linux-gnu)');renv::consent(provided=TRUE);renv::restore(confirm=FALSE)"

ENTRYPOINT [ "R","--no-save","--no-restore","--no-environ","--slave","-f","main.R", "--args"]
CMD [ "--taskId", "someid", "--serviceUri", "https://tercen.com", "--token", "sometoken"]

