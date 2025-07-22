/*
 * (C) 2023 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#ifndef DIASPORA_API_FACTORY_HPP
#define DIASPORA_API_FACTORY_HPP

#include <diaspora/ForwardDcl.hpp>
#include <diaspora/Exception.hpp>
#include <dlfcn.h>
#include <unordered_map>
#include <functional>
#include <memory>
#include <vector>

namespace diaspora {

template <typename Base, typename... Args>
class Factory;

template <typename FactoryType, typename Derived>
struct Registrar;

template <typename Base, typename... Args>
class Factory {

    public:

    static std::shared_ptr<Base> create(const std::string& key, Args&&... args);

private:

    template <typename FactoryType, typename Derived>
    friend struct Registrar;

    using CreatorFunction = std::function<std::shared_ptr<Base>(Args...)>;

    static Factory& instance();

    void registerCreator(const std::string& key, CreatorFunction creator);

    std::unordered_map<std::string, CreatorFunction> m_creator_fn;
};

template <typename FactoryType, typename Derived>
struct Registrar {

    explicit Registrar(const std::string& key) {
        auto& factory = FactoryType::instance();
        factory.registerCreator(key, Derived::create);
    }

};

}

#define DIASPORA_REGISTER_IMPLEMENTATION_FOR(__factory__, __derived__, __name__) \
    static ::diaspora::Registrar<::diaspora::__factory__, __derived__> \
    __diasporaRegistrarFor ## __factory__ ## _ ## __derived__ ## _ ## __name__{#__name__}

#endif
