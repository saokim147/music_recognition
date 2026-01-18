import torchvision.models as models

from models.resnet import resnet34, resnet_face18, resnet_face34, resnet_face50, resnet18


model_registry = {
    "vgg16": models.vgg16(pretrained=False),
    "vgg19": models.vgg19(pretrained=False),
    "resnet_face18": resnet_face18(use_se=False),
    "resnet_face34": resnet_face34(use_se=False),
    "resnet_face50": resnet_face50(use_se=False),
    "resnet18": resnet18(pretrained=False),
    "resnet34": resnet34(pretrained=False),
}


def get_model(model_name: str):
    model_cls = model_registry.get(model_name)
    if model_cls is None:
        raise ValueError(f"Unknown model: {model_name}. Available: {list(model_registry.keys())}")
    return model_cls
