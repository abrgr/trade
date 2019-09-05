FROM clojure:lein-2.8.3-alpine

RUN apk --skip-cache add gcc g++ gfortran patch wget make \
    && echo "Install Ipopt (https://www.coin-or.org/Ipopt/documentation/node10.html)" \
    && mkdir /scratch && cd /scratch \
    && wget https://www.coin-or.org/download/source/Ipopt/Ipopt-3.12.13.tgz \
    && tar -zxf Ipopt-3.12.13.tgz \
    && cd Ipopt-3.12.13 \
    && cd ThirdParty/Blas && ./get.Blas && cd - \
    && cd ThirdParty/Lapack && ./get.Lapack && cd - \
    && cd ThirdParty/ASL && ./get.ASL && cd - \
    && cd ThirdParty/Mumps && ./get.Mumps && cd - \
    && cd ThirdParty/Metis && ./get.Metis && cd - \
    && mkdir build && cd build \
    && ../configure && make && make install \
    && cp lib/libipopt.so /usr/lib/ \
    && cp /usr/lib/jvm/java-1.8-openjdk/include/linux/* /usr/lib/jvm/java-1.8-openjdk/include/ \
    && export JAVA_HOME=/usr/lib/jvm/java-1.8-openjdk \
    && cd Ipopt/contrib/JavaInterface \
    && make \
    && cp lib/libjipopt.so /usr/lib \
    && echo "Install JOM (http://www.net2plan.com/jom/index.php)" \
    && mkdir /scratch/jom && cd /scratch/jom \
    && wget https://github.com/girtel/JOM/releases/download/0.4.0/jom-0.4.0.jar \
    && wget http://www.net2plan.com/jom/externalsoftware/parallelcolt-0.9.4.jar
