/*
 * (C) 2023 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#include <diaspora/Factory.hpp>
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
std::shared_ptr<Base> Factory<Base, Args...>::create(const std::string& key, Args&&... args) {
    auto& factory = instance();
    std::string name = key;
    std::size_t found = key.find(":");
    if (found != std::string::npos) {
        name = key.substr(0, found);
        const auto path = key.substr(found + 1);
        auto it = factory.m_creator_fn.find(name);
        if (it == factory.m_creator_fn.end()) {
            if(dlopen(path.c_str(), RTLD_NOW) == nullptr) {
                throw Exception(
                        std::string{"Could not dlopen "} + path + ":" + dlerror());
            }
        }
    }
    auto it = factory.m_creator_fn.find(name);
    if (it != factory.m_creator_fn.end()) {
        return it->second(std::forward<Args>(args)...);
    } else {
        throw Exception(std::string("Factory method not found for type ") +  name);
    }
}

template <typename Base, typename... Args>
Factory<Base, Args...>& Factory<Base, Args...>::instance() {
    static Factory factory;
    return factory;
}

template <typename Base, typename... Args>
void Factory<Base, Args...>::registerCreator(const std::string& key, CreatorFunction creator) {
    m_creator_fn[key] = std::move(creator);
}

/* force instantiation of templates */
template class Factory<DriverInterface, const Metadata&>;
template class Factory<ValidatorInterface, const Metadata&>;
template class Factory<SerializerInterface, const Metadata&>;
template class Factory<PartitionSelectorInterface, const Metadata&>;

}
