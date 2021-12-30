import os.path
import warnings

from skimage.io import imread
from skimage.transform import resize

from .test_ai import *  # noqa


def load_image():
    image_filename = os.path.join(MODEL_DIR, dog_img)
    img_height, img_width = 224, 224
    img = imread(image_filename)
    img = resize(img, (img_height, img_width), mode="constant", anti_aliasing=True)
    img = img.astype(np.uint8)
    return img


@pytest.fixture
def client(modclient):
    modclient.flushdb()
    model_path = os.path.join(MODEL_DIR, torch_graph)
    ptmodel = load_model(model_path)
    modclient.ai().modelstore("pt_model", "torch", "cpu", ptmodel, tag="v7.0")
    return modclient


def test_deprecated_dugrun(client):
    # test the warning of using dagrun
    with warnings.catch_warnings(record=True) as w:
        warnings.simplefilter("default")
        dag = client.ai().dag()
    assert issubclass(w[-1].category, DeprecationWarning)

    # test that dagrun and model run hadn't been broken
    dag.tensorset("a", [2, 3, 2, 3], shape=(2, 2), dtype="float")
    dag.tensorset("b", [2, 3, 2, 3], shape=(2, 2), dtype="float")
    # can't use modelexecute or scriptexecute when using DAGRUN
    with pytest.raises(RuntimeError):
        dag.modelexecute("pt_model", ["a", "b"], ["output"])
    with pytest.raises(RuntimeError):
        dag.scriptexecute(
            "myscript{1}", "bar", inputs=["a{1}", "b{1}"], outputs=["c{1}"]
        )

    with pytest.deprecated_call():
        dag.modelrun("pt_model", ["a", "b"], ["output"])
    dag.tensorget("output")
    result = dag.execute()
    expected = [
        "OK",
        "OK",
        "OK",
        np.array([[4.0, 6.0], [4.0, 6.0]], dtype=np.float32),
    ]
    assert np.allclose(expected.pop(), result.pop())
    assert expected == result


def test_deprecated_modelrun_and_run(client):
    # use modelrun&run method but perform modelexecute&dagexecute behind the scene
    client.ai().tensorset("a", [2, 3, 2, 3], shape=(2, 2), dtype="float")
    client.ai().tensorset("b", [2, 3, 2, 3], shape=(2, 2), dtype="float")
    dag = client.ai().dag(load=["a", "b"], persist="output")
    with pytest.deprecated_call():
        dag.modelrun("pt_model", ["a", "b"], ["output"])
    dag.tensorget("output")
    result = dag.execute()
    expected = ["OK", np.array([[4.0, 6.0], [4.0, 6.0]], dtype=np.float32)]
    result_outside_dag = client.ai().tensorget("output")
    assert np.allclose(expected.pop(), result.pop())
    result = dag.execute()
    assert np.allclose(result_outside_dag, result.pop())
    assert expected == result


def test_dagexecute_with_scriptexecute_redis_commands(client):
    client.ai().scriptstore("myscript{1}", "cpu", script_with_redis_commands, "func")
    dag = client.ai().dag(persist="my_output{1}", routing="{1}")
    dag.tensorset("mytensor1{1}", [40], dtype="float")
    dag.tensorset("mytensor2{1}", [10], dtype="float")
    dag.tensorset("mytensor3{1}", [1], dtype="float")
    dag.scriptexecute(
        "myscript{1}",
        "func",
        keys=["key{1}"],
        inputs=["mytensor1{1}", "mytensor2{1}", "mytensor3{1}"],
        args=["3"],
        outputs=["my_output{1}"],
    )
    dag.execute()
    values = client.ai().tensorget("my_output{1}", as_numpy=False)
    assert np.allclose(values["values"], [54])


def test_dagexecute_modelexecute_with_scriptexecute(client):
    script_name = "imagenet_script:{1}"
    model_name = "imagenet_model:{1}"

    img = load_image()
    model_path = os.path.join(MODEL_DIR, "resnet50.pb")
    model = load_model(model_path)
    client.ai().scriptstore(
        script_name,
        "cpu",
        data_processing_script,
        entry_points=["post_process", "pre_process_3ch"],
    )
    client.ai().modelstore(
        model_name, "TF", "cpu", model, inputs="images", outputs="output"
    )

    dag = client.ai().dag(persist="output:{1}")
    dag.tensorset(
        "image:{1}", tensor=img, shape=(img.shape[1], img.shape[0]), dtype="UINT8"
    )
    dag.scriptexecute(
        script_name, "pre_process_3ch", inputs="image:{1}", outputs="temp_key1"
    )
    dag.modelexecute(model_name, inputs="temp_key1", outputs="temp_key2")
    dag.scriptexecute(
        script_name, "post_process", inputs="temp_key2", outputs="output:{1}"
    )
    ret = dag.execute()
    assert ["OK", "OK", "OK", "OK"] == ret


def test_dagexecute_with_load(client):
    client.ai().tensorset("a", [2, 3, 2, 3], shape=(2, 2), dtype="float")
    dag = client.ai().dag(load="a")
    dag.tensorset("b", [2, 3, 2, 3], shape=(2, 2), dtype="float")
    dag.modelexecute("pt_model", ["a", "b"], ["output"])
    dag.tensorget("output")
    result = dag.execute()
    expected = ["OK", "OK", np.array([[4.0, 6.0], [4.0, 6.0]], dtype=np.float32)]
    assert np.allclose(expected.pop(), result.pop())
    assert expected == result
    pytest.raises(ResponseError, client.ai().tensorget, "b")


def test_dagexecute_with_persist(client):
    with pytest.raises(ResponseError):
        dag = client.ai().dag(persist="wrongkey")
        dag.tensorset("a", [2, 3, 2, 3], shape=(2, 2), dtype="float").execute()

    dag = client.ai().dag(persist=["b"])
    dag.tensorset("a", [2, 3, 2, 3], shape=(2, 2), dtype="float")
    dag.tensorset("b", [2, 3, 2, 3], shape=(2, 2), dtype="float")
    dag.tensorget("b")
    result = dag.execute()
    b = client.ai().tensorget("b")
    assert np.allclose(b, result[-1])
    assert b.dtype == np.float32
    assert len(result) == 3


def test_dagexecute_calling_on_return(client):
    client.ai().tensorset("a", [2, 3, 2, 3], shape=(2, 2), dtype="float")
    result = (
        client.ai()
        .dag(load="a")
        .tensorset("b", [2, 3, 2, 3], shape=(2, 2), dtype="float")
        .modelexecute("pt_model", ["a", "b"], ["output"])
        .tensorget("output")
        .execute()
    )
    expected = ["OK", "OK", np.array([[4.0, 6.0], [4.0, 6.0]], dtype=np.float32)]
    assert np.allclose(expected.pop(), result.pop())
    assert expected == result


def test_dagexecute_without_load_and_persist(client):
    dag = client.ai().dag(load="wrongkey")
    with pytest.raises(ResponseError):
        dag.tensorget("wrongkey").execute()

    dag = client.ai().dag(persist="output")
    dag.tensorset("a", [2, 3, 2, 3], shape=(2, 2), dtype="float")
    dag.tensorset("b", [2, 3, 2, 3], shape=(2, 2), dtype="float")
    dag.modelexecute("pt_model", ["a", "b"], ["output"])
    dag.tensorget("output")
    result = dag.execute()
    expected = [
        "OK",
        "OK",
        "OK",
        np.array([[4.0, 6.0], [4.0, 6.0]], dtype=np.float32),
    ]
    assert np.allclose(expected.pop(), result.pop())
    assert expected == result


def test_dagexecute_with_load_and_persist(client):
    client.ai().tensorset("a", [2, 3, 2, 3], shape=(2, 2), dtype="float")
    client.ai().tensorset("b", [2, 3, 2, 3], shape=(2, 2), dtype="float")
    dag = client.ai().dag(load=["a", "b"], persist="output")
    dag.modelexecute("pt_model", ["a", "b"], ["output"])
    dag.tensorget("output")
    result = dag.execute()
    expected = ["OK", np.array([[4.0, 6.0], [4.0, 6.0]], dtype=np.float32)]
    result_outside_dag = client.ai().tensorget("output")
    assert np.allclose(expected.pop(), result.pop())
    result = dag.execute()
    assert np.allclose(result_outside_dag, result.pop())
    assert expected == result


def test_dagexecuteRO(client):
    client.ai().tensorset("a", [2, 3, 2, 3], shape=(2, 2), dtype="float")
    client.ai().tensorset("b", [2, 3, 2, 3], shape=(2, 2), dtype="float")
    with pytest.raises(RuntimeError):
        client.ai().dag(load=["a", "b"], persist="output", readonly=True)
    dag = client.ai().dag(load=["a", "b"], readonly=True)

    with pytest.raises(RuntimeError):
        dag.scriptexecute(
            "myscript{1}", "bar", inputs=["a{1}", "b{1}"], outputs=["c{1}"]
        )

    dag.modelexecute("pt_model", ["a", "b"], ["output"])
    dag.tensorget("output")
    result = dag.execute()
    expected = ["OK", np.array([[4.0, 6.0], [4.0, 6.0]], dtype=np.float32)]
    assert np.allclose(expected.pop(), result.pop())
